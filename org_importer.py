#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Star Citizen Organization Importer
----------------------------------
An optimized tool to import and track Star Citizen organizations
and their members, with a history of changes.

Date: April 24, 2025
"""

import aiohttp
import asyncio
import json
import logging
import sqlite3
import time
from math import ceil
from typing import Dict, List, Optional, Tuple, Union, Any
from dataclasses import dataclass
from datetime import datetime
from tqdm import tqdm
from bs4 import BeautifulSoup
from contextlib import closing


# Logger configuration
logging.basicConfig(
    filename='sc_org_importer.log',
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("SCOrgImporter")


@dataclass
class Organization:
    """Represents a Star Citizen organization."""
    name: str
    symbol: str
    url_image: str
    url_corpo: str
    archetype: str
    langage: str  # Keep 'langage' if it's a specific field name, otherwise change to 'language'
    commitment: str
    recrutement: Optional[str] = None # Keep 'recrutement' if it's a specific field name, otherwise change to 'recruitment'
    role_play: Optional[str] = None
    nb_membres: Optional[str] = None # Keep 'nb_membres' if it's a specific field name, otherwise change to 'member_count'
    id: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        """Converts the object to a dictionary."""
        return {
            "id": self.id,
            "name": self.name,
            "symbol": self.symbol,
            "url_image": self.url_image,
            "url_corpo": self.url_corpo,
            "archetype": self.archetype,
            "langage": self.langage,
            "commitment": self.commitment,
            "recrutement": self.recrutement,
            "role_play": self.role_play,
            "nb_membres": self.nb_membres
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Organization':
        """Creates an Organization object from a dictionary."""
        return cls(
            id=data.get("id"),
            name=data.get("name"),
            symbol=data.get("symbol"),
            url_image=data.get("url_image"),
            url_corpo=data.get("url_corpo"),
            archetype=data.get("archetype"),
            langage=data.get("langage"),
            commitment=data.get("commitment"),
            recrutement=data.get("recrutement"),
            role_play=data.get("role_play"),
            nb_membres=data.get("nb_membres")
        )


@dataclass
class Member:
    """Represents a member of a Star Citizen organization."""
    name: str
    symbol: str
    url_image: str
    url_member: str
    rank: str = "N/A"
    id: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        """Converts the object to a dictionary."""
        return {
            "id": self.id,
            "name": self.name,
            "symbol": self.symbol,
            "url_image": self.url_image,
            "url_member": self.url_member,
            "rank": self.rank
        }


class DatabaseManager:
    """Manages database operations."""

    def __init__(self, db_path: str = 'sc_organizations.db'):
        """Initializes the database manager.

        Args:
            db_path: Path to the SQLite database.
        """
        self.db_path = db_path
        self.connection = None
        self.cursor = None

    def connect(self) -> None:
        """Establishes a connection to the database."""
        self.connection = sqlite3.connect(self.db_path)
        self.connection.row_factory = sqlite3.Row
        self.cursor = self.connection.cursor()

    def disconnect(self) -> None:
        """Closes the database connection."""
        if self.connection:
            self.cursor.close()
            self.connection.close()
            self.connection = None
            self.cursor = None

    def setup_database(self) -> None:
        """Configures the database structure."""
        # Organizations table
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS organizations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            symbol TEXT NOT NULL UNIQUE,
            url_image TEXT,
            url_corpo TEXT,
            archetype TEXT,
            language TEXT,
            commitment TEXT,
            recruitment TEXT,
            role_play TEXT,
            member_count TEXT,
            members_updated BOOLEAN DEFAULT 0,
            is_active BOOLEAN DEFAULT 1,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)

        # Members table
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS members (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            symbol TEXT NOT NULL UNIQUE,
            url_image TEXT,
            url_member TEXT,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)

        # Organization history table
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS organization_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            organization_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            symbol TEXT NOT NULL,
            url_image TEXT,
            url_corpo TEXT,
            archetype TEXT,
            language TEXT,
            commitment TEXT,
            recruitment TEXT,
            role_play TEXT,
            member_count TEXT,
            change_description TEXT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (organization_id) REFERENCES organizations(id)
        )
        """)

        # Member-organization association table with history
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS member_organization (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            member_id INTEGER NOT NULL,
            organization_id INTEGER NOT NULL,
            rank TEXT,
            joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            left_at TIMESTAMP,
            is_active BOOLEAN DEFAULT 1,
            FOREIGN KEY (member_id) REFERENCES members(id),
            FOREIGN KEY (organization_id) REFERENCES organizations(id),
            UNIQUE(member_id, organization_id, is_active)
        )
        """)

        # Member rank history table
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS member_rank_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            member_id INTEGER NOT NULL,
            organization_id INTEGER NOT NULL,
            rank TEXT NOT NULL,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (member_id) REFERENCES members(id),
            FOREIGN KEY (organization_id) REFERENCES organizations(id)
        )
        """)

        # Indices for performance improvement
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_org_symbol ON organizations (symbol)")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_member_symbol ON members (symbol)")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_member_org ON member_organization (member_id, organization_id)")

        self.connection.commit()

    def migrate_from_old_db(self, old_db_path: str) -> None:
        """Migrates data from the old database structure.

        Args:
            old_db_path: Path to the old database.
        """
        try:
            old_conn = sqlite3.connect(old_db_path)
            old_conn.row_factory = sqlite3.Row
            old_cursor = old_conn.cursor()

            # Check if necessary tables exist
            old_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='Corporations';")
            if old_cursor.fetchone() is None:
                logger.error("The 'Corporations' table does not exist in the source database.")
                return

            # Migrate organizations
            logger.info("Migrating organizations from the old database...")
            old_cursor.execute("SELECT * FROM Corporations")
            orgs = old_cursor.fetchall()

            for org in tqdm(orgs, desc="Migrating organizations"):
                self.cursor.execute("""
                    INSERT OR IGNORE INTO organizations (
                        name, symbol, url_image, url_corpo, archetype,
                        language, commitment, recruitment, role_play, member_count,
                        members_updated, is_active, last_updated
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    org['name'], org['symbol'], org['url_image'], org['url_corpo'],
                    org['archetype'], org['langage'], org['commitment'],
                    org['recrutement'], org['role_play'], org['nb_membres'],
                    bool(org['members_updated']), not bool(org['orga_null']), org['timestamp']
                ))

                # Get the organization ID in the new database
                self.cursor.execute("SELECT id FROM organizations WHERE symbol = ?", (org['symbol'],))
                result = self.cursor.fetchone()

                if result:
                    new_org_id = result['id']

                    # Migrate organization history
                    old_cursor.execute("SELECT * FROM CorporationHistory WHERE corporation_id = ?", (org['id'],))
                    history_items = old_cursor.fetchall()

                    for item in history_items:
                        self.cursor.execute("""
                            INSERT INTO organization_history (
                                organization_id, name, symbol, url_image, url_corpo,
                                archetype, language, commitment, recruitment, role_play,
                                member_count, timestamp
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            new_org_id, item['name'], item['symbol'], item['url_image'],
                            item['url_corpo'], item['archetype'], item['langage'],
                            item['commitment'], item['recrutement'], item['role_play'],
                            item['nb_membres'], item['timestamp']
                        ))

            # Migrate members
            logger.info("Migrating members from the old database...")
            old_cursor.execute("SELECT * FROM Members")
            members = old_cursor.fetchall()

            # Create a mapping table from symbols to IDs
            symbol_to_id_map = {}

            for member in tqdm(members, desc="Migrating members"):
                self.cursor.execute("""
                    INSERT OR IGNORE INTO members (
                        name, symbol, url_image, url_member, last_updated
                    ) VALUES (?, ?, ?, ?, ?)
                """, (
                    member['name'], member['symbol'], member['url_image'],
                    member['url_member'], member['timestamp']
                ))

                # Get the member ID in the new database
                self.cursor.execute("SELECT id FROM members WHERE symbol = ?", (member['symbol'],))
                new_id = self.cursor.fetchone()['id']

                # Store the old ID -> new ID mapping
                symbol_to_id_map[member['id']] = new_id

            # Migrate member-organization associations
            logger.info("Migrating member-organization associations...")
            old_cursor.execute("""
                SELECT DISTINCT mch.id_member, mch.id_corpo, mch.rank, mch.timestamp,
                               c.symbol as corpo_symbol, m.symbol as member_symbol
                FROM Memberscorpohistory mch
                JOIN Members m ON mch.id_member = m.id
                JOIN Corporations c ON mch.id_corpo = c.id
            """)
            member_orgs = old_cursor.fetchall()

            for mo in tqdm(member_orgs, desc="Migrating member-organization associations"):
                # Get IDs in the new database
                self.cursor.execute("SELECT id FROM members WHERE symbol = ?", (mo['member_symbol'],))
                member_result = self.cursor.fetchone()

                self.cursor.execute("SELECT id FROM organizations WHERE symbol = ?", (mo['corpo_symbol'],))
                org_result = self.cursor.fetchone()

                if member_result and org_result:
                    new_member_id = member_result['id']
                    new_org_id = org_result['id']

                    # Insert into member_organization
                    self.cursor.execute("""
                        INSERT OR IGNORE INTO member_organization (
                            member_id, organization_id, rank, joined_at
                        ) VALUES (?, ?, ?, ?)
                    """, (new_member_id, new_org_id, mo['rank'], mo['timestamp']))

                    # Insert into member_rank_history
                    self.cursor.execute("""
                        INSERT INTO member_rank_history (
                            member_id, organization_id, rank, timestamp
                        ) VALUES (?, ?, ?, ?)
                    """, (new_member_id, new_org_id, mo['rank'], mo['timestamp']))

            self.connection.commit()
            logger.info("Migration completed successfully!")

        except sqlite3.Error as e:
            logger.error(f"Error during migration: {e}")
            self.connection.rollback()
            raise
        finally:
            if 'old_conn' in locals():
                old_conn.close()

    def save_organization(self, org: Organization) -> Tuple[int, bool]:
        """Saves an organization to the database.

        Args:
            org: The Organization object to save.

        Returns:
            Tuple containing the organization ID and a boolean indicating if it's a new entry.
        """
        try:
            self.cursor.execute("SELECT * FROM organizations WHERE symbol = ?", (org.symbol,))
            existing_org = self.cursor.fetchone()

            is_new = existing_org is None
            changes = []

            if is_new:
                # Insert a new organization
                self.cursor.execute("""
                    INSERT INTO organizations (
                        name, symbol, url_image, url_corpo, archetype,
                        language, commitment, recruitment, role_play, member_count
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    org.name, org.symbol, org.url_image, org.url_corpo, org.archetype,
                    org.langage, org.commitment, org.recrutement, org.role_play, org.nb_membres
                ))
                org_id = self.cursor.lastrowid

                # Add to history
                self.cursor.execute("""
                    INSERT INTO organization_history (
                        organization_id, name, symbol, url_image, url_corpo,
                        archetype, language, commitment, recruitment, role_play,
                        member_count, change_description
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    org_id, org.name, org.symbol, org.url_image, org.url_corpo,
                    org.archetype, org.langage, org.commitment, org.recrutement,
                    org.role_play, org.nb_membres, "Organization created"
                ))
            else:
                # Existing organization, check for changes
                org_id = existing_org['id']

                # Compare attributes to detect changes
                if org.name != existing_org['name']:
                    changes.append(f"Name: {existing_org['name']} -> {org.name}")

                if org.url_image != existing_org['url_image']:
                    changes.append(f"Image URL: {existing_org['url_image']} -> {org.url_image}")

                if org.url_corpo != existing_org['url_corpo']:
                    changes.append(f"Org URL: {existing_org['url_corpo']} -> {org.url_corpo}")

                if org.archetype != existing_org['archetype']:
                    changes.append(f"Archetype: {existing_org['archetype']} -> {org.archetype}")

                if org.langage != existing_org['language']:
                    changes.append(f"Language: {existing_org['language']} -> {org.langage}")

                if org.commitment != existing_org['commitment']:
                    changes.append(f"Commitment: {existing_org['commitment']} -> {org.commitment}")

                if org.recrutement != existing_org['recruitment']:
                    changes.append(f"Recruitment: {existing_org['recruitment']} -> {org.recrutement}")

                if org.role_play != existing_org['role_play']:
                    changes.append(f"Roleplay: {existing_org['role_play']} -> {org.role_play}")

                if org.nb_membres != existing_org['member_count']:
                    changes.append(f"Member count: {existing_org['member_count']} -> {org.nb_membres}")

                # If there are changes, update the organization and history
                if changes:
                    change_desc = ", ".join(changes)
                    logger.info(f"Changes detected for {org.symbol}: {change_desc}")

                    # Update the organization
                    self.cursor.execute("""
                        UPDATE organizations SET
                        name = ?, url_image = ?, url_corpo = ?, archetype = ?,
                        language = ?, commitment = ?, recruitment = ?, role_play = ?,
                        member_count = ?, last_updated = CURRENT_TIMESTAMP
                        WHERE id = ?
                    """, (
                        org.name, org.url_image, org.url_corpo, org.archetype,
                        org.langage, org.commitment, org.recrutement, org.role_play,
                        org.nb_membres, org_id
                    ))

                    # Add to history
                    self.cursor.execute("""
                        INSERT INTO organization_history (
                            organization_id, name, symbol, url_image, url_corpo,
                            archetype, language, commitment, recruitment, role_play,
                            member_count, change_description
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        org_id, org.name, org.symbol, org.url_image, org.url_corpo,
                        org.archetype, org.langage, org.commitment, org.recrutement,
                        org.role_play, org.nb_membres, change_desc
                    ))
                else:
                    # No changes, just update the last checked date
                    self.cursor.execute("""
                        UPDATE organizations SET last_updated = CURRENT_TIMESTAMP
                        WHERE id = ?
                    """, (org_id,))

            self.connection.commit()
            return org_id, is_new

        except sqlite3.Error as e:
            logger.error(f"Error saving organization {org.symbol}: {e}")
            self.connection.rollback()
            raise

    def save_member(self, member: Member) -> int:
        """Saves a member to the database.

        Args:
            member: The Member object to save.

        Returns:
            The member ID.
        """
        try:
            self.cursor.execute("SELECT id FROM members WHERE symbol = ?", (member.symbol,))
            existing_member = self.cursor.fetchone()

            if existing_member:
                # Update existing member
                self.cursor.execute("""
                    UPDATE members SET
                    name = ?, url_image = ?, url_member = ?, last_updated = CURRENT_TIMESTAMP
                    WHERE id = ?
                """, (member.name, member.url_image, member.url_member, existing_member['id']))
                member_id = existing_member['id']
            else:
                # Insert new member
                self.cursor.execute("""
                    INSERT INTO members (name, symbol, url_image, url_member)
                    VALUES (?, ?, ?, ?)
                """, (member.name, member.symbol, member.url_image, member.url_member))
                member_id = self.cursor.lastrowid

            self.connection.commit()
            return member_id

        except sqlite3.Error as e:
            logger.error(f"Error saving member {member.symbol}: {e}")
            self.connection.rollback()
            raise

    def save_member_organization(self, member_id: int, org_id: int, rank: str) -> None:
        """Saves the association between a member and an organization.

        Args:
            member_id: The member ID.
            org_id: The organization ID.
            rank: The member's rank in the organization.
        """
        try:
            # Check if the association already exists
            self.cursor.execute("""
                SELECT id, rank FROM member_organization
                WHERE member_id = ? AND organization_id = ? AND is_active = 1
            """, (member_id, org_id))
            existing = self.cursor.fetchone()

            if existing:
                # Check if the rank has changed
                if existing['rank'] != rank:
                    logger.info(f"Rank change detected for member {member_id} in organization {org_id}: {existing['rank']} -> {rank}")

                    # Update rank in member_organization
                    self.cursor.execute("""
                        UPDATE member_organization SET rank = ?
                        WHERE id = ?
                    """, (rank, existing['id']))

                    # Add entry to member_rank_history
                    self.cursor.execute("""
                        INSERT INTO member_rank_history (member_id, organization_id, rank)
                        VALUES (?, ?, ?)
                    """, (member_id, org_id, rank))
            else:
                # New association
                self.cursor.execute("""
                    INSERT INTO member_organization (member_id, organization_id, rank)
                    VALUES (?, ?, ?)
                """, (member_id, org_id, rank))

                # Add entry to member_rank_history
                self.cursor.execute("""
                    INSERT INTO member_rank_history (member_id, organization_id, rank)
                    VALUES (?, ?, ?)
                """, (member_id, org_id, rank))

            self.connection.commit()

        except sqlite3.Error as e:
            logger.error(f"Error saving member-organization association: {e}")
            self.connection.rollback()
            raise

    def mark_member_left_organization(self, member_id: int, org_id: int) -> None:
        """Marks that a member has left an organization.

        Args:
            member_id: The member ID.
            org_id: The organization ID.
        """
        try:
            # First check if the member is already marked as having left the organization
            self.cursor.execute("""
                SELECT id FROM member_organization
                WHERE member_id = ? AND organization_id = ? AND is_active = 0
            """, (member_id, org_id))

            if self.cursor.fetchone():
                # Member is already marked as having left this organization
                logger.debug(f"Member {member_id} was already marked as having left organization {org_id}")
                return

            # Update only if an active entry exists
            result = self.cursor.execute("""
                UPDATE member_organization SET
                is_active = 0, left_at = CURRENT_TIMESTAMP
                WHERE member_id = ? AND organization_id = ? AND is_active = 1
            """, (member_id, org_id))

            if result.rowcount == 0:
                logger.debug(f"No active entry found for member {member_id} in organization {org_id}")

            self.connection.commit()

        except sqlite3.Error as e:
            logger.error(f"Error marking departure of member {member_id} from organization {org_id}: {e}")
            self.connection.rollback()
            # Do not re-raise the exception to avoid interrupting the import

    def mark_organization_members_updated(self, org_id: int) -> None:
        """Marks that an organization has had its members updated.

        Args:
            org_id: The organization ID.
        """
        try:
            self.cursor.execute("""
                UPDATE organizations SET
                members_updated = 1
                WHERE id = ?
            """, (org_id,))
            self.connection.commit()

        except sqlite3.Error as e:
            logger.error(f"Error marking organization {org_id} members as updated: {e}")
            self.connection.rollback()
            raise

    def reset_members_updated_flag(self) -> None:
        """Resets the members_updated flag for all organizations."""
        try:
            self.cursor.execute("UPDATE organizations SET members_updated = 0")
            self.connection.commit()

        except sqlite3.Error as e:
            logger.error(f"Error resetting members_updated flags: {e}")
            self.connection.rollback()
            raise

    def get_organizations_to_update(self, hours: int = 1) -> List[Dict[str, Any]]:
        """Retrieves organizations that haven't been updated in a while.

        Args:
            hours: Number of hours since the last update.

        Returns:
            List of organizations to update.
        """
        try:
            self.cursor.execute("""
                SELECT id, symbol FROM organizations
                WHERE last_updated <= datetime('now', ? || ' hours')
                AND is_active = 1
            """, (f"-{hours}",))
            return [dict(row) for row in self.cursor.fetchall()]

        except sqlite3.Error as e:
            logger.error(f"Error retrieving organizations to update: {e}")
            raise

    def get_organizations_for_member_update(self) -> List[Dict[str, Any]]:
        """Retrieves organizations whose members need to be updated.

        Returns:
            List of organizations whose members need to be updated.
        """
        try:
            self.cursor.execute("""
                SELECT id, symbol FROM organizations
                WHERE members_updated = 0 AND is_active = 1
                ORDER BY id ASC
            """)
            return [dict(row) for row in self.cursor.fetchall()]

        except sqlite3.Error as e:
            logger.error(f"Error retrieving organizations for member update: {e}")
            raise

    def get_current_members(self, org_id: int) -> Dict[str, int]:
        """Retrieves the current members of an organization.

        Args:
            org_id: The organization ID.

        Returns:
            Dictionary of current members with their IDs.
        """
        try:
            self.cursor.execute("""
                SELECT m.id, m.symbol
                FROM members m
                JOIN member_organization mo ON m.id = mo.member_id
                WHERE mo.organization_id = ? AND mo.is_active = 1
            """, (org_id,))
            return {row['symbol']: row['id'] for row in self.cursor.fetchall()}

        except sqlite3.Error as e:
            logger.error(f"Error retrieving current members of organization {org_id}: {e}")
            raise

    def get_all_member_symbols(self) -> Dict[str, int]:
        """Retrieves all member symbols and their IDs from the database.

        Returns:
            Dictionary mapping member symbol to member ID.
        """
        try:
            self.cursor.execute("SELECT id, symbol FROM members")
            return {row['symbol']: row['id'] for row in self.cursor.fetchall()}
        except sqlite3.Error as e:
            logger.error(f"Error retrieving all member symbols: {e}")
            raise

    def get_active_org_members_with_rank(self, org_id: int) -> Dict[int, Tuple[str, int]]:
        """Retrieves active members, their ranks, and association IDs for a specific organization.

        Args:
            org_id: The organization ID.

        Returns:
            Dictionary mapping member ID to a tuple of (current rank, association ID).
        """
        try:
            self.cursor.execute("""
                SELECT id, member_id, rank
                FROM member_organization
                WHERE organization_id = ? AND is_active = 1
            """, (org_id,))
            # Map member_id -> (rank, association_id)
            return {row['member_id']: (row['rank'], row['id']) for row in self.cursor.fetchall()}
        except sqlite3.Error as e:
            logger.error(f"Error retrieving active members for organization {org_id}: {e}")
            raise

    def get_member_name(self, member_id: int) -> str:
        """Retrieves a member's name from their ID.

        Args:
            member_id: The member ID.

        Returns:
            The member's name or an empty string if not found.
        """
        try:
            self.cursor.execute("""
                SELECT name, symbol FROM members
                WHERE id = ?
            """, (member_id,))
            result = self.cursor.fetchone()
            return result['name'] if result else ""

        except sqlite3.Error as e:
            logger.error(f"Error retrieving name for member {member_id}: {e}")
            return ""

    # --- Batch Operations --- #

    def batch_insert_members(self, members_to_insert: List[Member]) -> Dict[str, int]:
        """Inserts multiple new members in a batch.

        Args:
            members_to_insert: List of Member objects to insert.

        Returns:
            Dictionary mapping symbol to newly inserted member ID.
        """
        if not members_to_insert:
            return {}

        new_member_ids = {}
        try:
            data = [
                (m.name, m.symbol, m.url_image, m.url_member)
                for m in members_to_insert
            ]
            self.cursor.executemany("""
                INSERT INTO members (name, symbol, url_image, url_member)
                VALUES (?, ?, ?, ?)
            """, data)

            # Retrieve IDs for the inserted members (less efficient, but needed)
            # Assumes symbols are unique and were just inserted
            symbols = [m.symbol for m in members_to_insert]
            # Create placeholders for the IN clause
            placeholders = ', '.join('?' * len(symbols))
            self.cursor.execute(f"SELECT id, symbol FROM members WHERE symbol IN ({placeholders})", symbols)
            new_member_ids = {row['symbol']: row['id'] for row in self.cursor.fetchall()}
            logger.debug(f"Batch inserted {len(data)} new members.")
            return new_member_ids
        except sqlite3.Error as e:
            logger.error(f"Error batch inserting members: {e}")
            raise # Re-raise to trigger rollback

    def batch_update_members(self, members_to_update: List[Member]) -> None:
        """Updates multiple existing members in a batch.

        Args:
            members_to_update: List of Member objects with IDs to update.
        """
        if not members_to_update:
            return

        try:
            data = [
                (m.name, m.url_image, m.url_member, m.id)
                for m in members_to_update
            ]
            self.cursor.executemany("""
                UPDATE members SET
                name = ?, url_image = ?, url_member = ?, last_updated = CURRENT_TIMESTAMP
                WHERE id = ?
            """, data)
            logger.debug(f"Batch updated {len(data)} members.")
        except sqlite3.Error as e:
            logger.error(f"Error batch updating members: {e}")
            raise # Re-raise to trigger rollback

    def batch_insert_associations(self, associations_to_insert: List[Tuple[int, int, str]]) -> None:
        """Inserts multiple new member-organization associations.

        Args:
            associations_to_insert: List of tuples (member_id, org_id, rank).
        """
        if not associations_to_insert:
            return

        try:
            self.cursor.executemany("""
                INSERT INTO member_organization (member_id, organization_id, rank)
                VALUES (?, ?, ?)
            """, associations_to_insert)
            logger.debug(f"Batch inserted {len(associations_to_insert)} new associations.")
        except sqlite3.Error as e:
            # Handle potential UNIQUE constraint violations if logic has overlap
            if "UNIQUE constraint failed" in str(e):
                logger.warning(f"Attempted to insert duplicate active associations: {e}")
            else:
                logger.error(f"Error batch inserting associations: {e}")
                raise # Re-raise to trigger rollback

    def batch_update_associations_rank(self, associations_to_update: List[Tuple[str, int]]) -> None:
        """Updates the rank for multiple existing member-organization associations.

        Args:
            associations_to_update: List of tuples (new_rank, association_id).
        """
        if not associations_to_update:
            return

        try:
            self.cursor.executemany("""
                UPDATE member_organization SET rank = ?
                WHERE id = ?
            """, associations_to_update)
            logger.debug(f"Batch updated rank for {len(associations_to_update)} associations.")
        except sqlite3.Error as e:
            logger.error(f"Error batch updating association ranks: {e}")
            raise # Re-raise to trigger rollback

    def batch_insert_rank_history(self, rank_history_to_insert: List[Tuple[int, int, str]]) -> None:
        """Inserts multiple rank history entries.

        Args:
            rank_history_to_insert: List of tuples (member_id, org_id, rank).
        """
        if not rank_history_to_insert:
            return

        try:
            self.cursor.executemany("""
                INSERT INTO member_rank_history (member_id, organization_id, rank)
                VALUES (?, ?, ?)
            """, rank_history_to_insert)
            logger.debug(f"Batch inserted {len(rank_history_to_insert)} rank history entries.")
        except sqlite3.Error as e:
            logger.error(f"Error batch inserting rank history: {e}")
            raise # Re-raise to trigger rollback

    def batch_mark_departures(self, departing_member_ids: List[int], org_id: int) -> None:
        """Marks multiple members as having left an organization.

        Args:
            departing_member_ids: List of member IDs that have left.
            org_id: The organization ID they left.
        """
        if not departing_member_ids:
            return

        try:
            data = [(org_id, member_id) for member_id in departing_member_ids]
            result = self.cursor.executemany("""
                UPDATE member_organization SET
                is_active = 0, left_at = CURRENT_TIMESTAMP
                WHERE organization_id = ? AND member_id = ? AND is_active = 1
            """, data)
            # Note: executemany doesn't reliably return rowcount in older sqlite versions
            logger.debug(f"Attempted to mark {len(departing_member_ids)} members as departed from org {org_id}.")
        except sqlite3.Error as e:
            logger.error(f"Error batch marking departures for org {org_id}: {e}")
            raise # Re-raise to trigger rollback

    # --- End Batch Operations --- #


class RSIApiClient:
    """Client for interacting with the Roberts Space Industries API."""

    BASE_URL = "https://robertsspaceindustries.com/api"

    def __init__(self, session: aiohttp.ClientSession):
        """Initializes the API client.

        Args:
            session: aiohttp session to use for requests.
        """
        self.session = session

    async def _make_request(self, endpoint: str, data: Dict[str, Any], max_retries: int = 5) -> Optional[Dict[str, Any]]:
        """Makes a request to the RSI API.

        Args:
            endpoint: API endpoint.
            data: Data to send with the request.
            max_retries: Maximum number of retries in case of error.

        Returns:
            JSON response data or None on failure.
        """
        headers = {"Content-Type": "application/json"}
        json_data = json.dumps(data)
        url = f"{self.BASE_URL}/{endpoint}"

        for attempt in range(max_retries):
            try:
                async with self.session.post(url, data=json_data, headers=headers) as response:
                    await asyncio.sleep(0.5)  # Respect API limits

                    if response.status != 200:
                        logger.error(f"HTTP error {response.status} for {url}")
                        time.sleep(2 ** attempt)  # Exponential backoff
                        continue

                    text = await response.text()
                    if not text:
                        logger.error("Empty response received from API.")
                        time.sleep(2 ** attempt)
                        continue

                    try:
                        data = json.loads(text)
                    except json.JSONDecodeError as e:
                        logger.error(f"JSON decoding error: {e}")
                        time.sleep(2 ** attempt)
                        continue

                    if data.get('code') == 'ErrApiThrottled':
                        wait_time = 5 * (2 ** attempt)  # Exponential backoff
                        logger.warning(f"API throttling detected. Retrying in {wait_time} seconds...")
                        time.sleep(wait_time)
                        continue

                    return data

            except aiohttp.ClientError as e:
                logger.error(f"HTTP client error: {e}")
                time.sleep(2 ** attempt)

        logger.error(f"Failed after {max_retries} attempts for {endpoint}")
        return None

    async def get_organizations(self, page: int, search: str = "", sort: str = "") -> Optional[List[Organization]]:
        """Retrieves organizations from the API.

        Args:
            page: Page number to retrieve.
            search: Search term (optional).
            sort: Sorting criteria (optional).

        Returns:
            List of retrieved organizations or None on failure.
        """
        data = {
            "sort": sort,
            "search": search,
            "commitment": [],
            "roleplay": [],
            "size": [],
            "model": [],
            "activity": [],
            "language": [],
            "recruiting": [],
            "pagesize": 12,
            "page": page
        }

        response = await self._make_request("orgs/getOrgs", data)
        if not response:
            return None

        if 'data' not in response or 'html' not in response['data']:
            logger.error("Unexpected response format from getOrgs API")
            return None

        soup = BeautifulSoup(response['data']['html'], "lxml")
        org_cells = soup.find_all("div", {"class": "org-cell"})

        if not org_cells:
            logger.info("No organizations found on this page")
            return None

        organizations = []

        for cell in org_cells:
            try:
                cell_soup = BeautifulSoup(str(cell), "lxml")

                # Image URL
                img_tag = cell_soup.find("span", {"class": "thumb"}).find("img")
                url_image = img_tag["src"] if img_tag else ""

                # Corporation URL
                url_tag = cell_soup.find("a", {"class": "trans-03s clearfix"})
                url_corpo = f"https://robertsspaceindustries.com{url_tag['href']}" if url_tag else ""

                # Name and symbol
                name_tag = cell_soup.find("h3", {"class": "trans-03s name"})
                name = name_tag.text if name_tag else ""

                symbol_tag = cell_soup.find("span", {"class": "symbol"})
                symbol = symbol_tag.text if symbol_tag else ""

                # Other information
                right_tags = cell_soup.find_all("span", {"class": "right"})
                info_values = []

                for right_tag in right_tags:
                    right_soup = BeautifulSoup(str(right_tag), "lxml")
                    values = right_soup.find_all("span", {"class": "value"})
                    info_values.extend([v.text for v in values])

                archetype = info_values[0] if len(info_values) > 0 else ""
                langage = info_values[1] if len(info_values) > 1 else ""
                commitment = info_values[2] if len(info_values) > 2 else ""
                recrutement = info_values[3] if len(info_values) > 3 else ""
                role_play = info_values[4] if len(info_values) > 4 else ""
                nb_membres = info_values[5] if len(info_values) > 5 else ""

                org = Organization(
                    name=name,
                    symbol=symbol,
                    url_image=url_image,
                    url_corpo=url_corpo,
                    archetype=archetype,
                    langage=langage,
                    commitment=commitment,
                    recrutement=recrutement,
                    role_play=role_play,
                    nb_membres=nb_membres
                )

                organizations.append(org)

            except Exception as e:
                logger.error(f"Error parsing an organization cell: {e}")
                continue

        return organizations

    async def get_organization_members(self, symbol: str) -> Optional[Tuple[List[Member], int]]:
        """Retrieves members of an organization.

        Args:
            symbol: Organization symbol.

        Returns:
            Tuple containing the list of members and the total member count, or None on failure.
        """
        data = {
            "symbol": symbol,
            "search": "",
            "pagesize": 32,
            "page": 1
        }

        response = await self._make_request("orgs/getOrgMembers", data)
        if not response:
            return None

        if response.get('code') == 'ErrInvalidOrganization':
            logger.warning(f"Organization {symbol} is invalid or does not exist")
            return ([], 0)

        if 'data' not in response or 'html' not in response['data'] or 'totalrows' not in response['data']:
            logger.error("Unexpected response format from getOrgMembers API")
            return None

        total_rows = response['data']['totalrows']
        total_pages = ceil(total_rows / 32)

        members = await self._parse_members_page(response['data']['html'], symbol) # Pass symbol here

        # Retrieve additional pages
        for page in range(2, total_pages + 1):
            data['page'] = page
            page_response = await self._make_request("orgs/getOrgMembers", data)

            if not page_response or 'data' not in page_response or 'html' not in page_response['data']:
                logger.error(f"Error retrieving page {page} of members for org '{symbol}'") # Added org symbol here too for context
                continue

            page_members = await self._parse_members_page(page_response['data']['html'], symbol) # Pass symbol here too
            members.extend(page_members)

        return (members, total_rows)

    async def _parse_members_page(self, html: str, org_symbol: str) -> List[Member]: # Add org_symbol parameter
        """Parses an HTML page of members.

        Args:
            html: HTML content to parse.
            org_symbol: The symbol of the organization being parsed.

        Returns:
            List of members extracted from the page.
        """
        members = []
        soup = BeautifulSoup(html, "lxml")
        member_items = soup.find_all("li", class_="member-item")

        if not member_items:
            member_items = soup.find_all("a", class_="membercard")

        for item in member_items:
            try:
                card = item if item.name == 'a' else item.find("a", class_="membercard")
                if not card:
                    continue

                # --- Extract URL ---
                url_member = card.get('href', '')
                if url_member and url_member.startswith('/'):
                    url_member = f"https://robertsspaceindustries.com{url_member}"
                # --- End Extract URL ---

                # --- Extract Image URL ---
                img_tag = card.find("span", class_="thumb").find("img") if card.find("span", class_="thumb") else None
                url_image = img_tag.get('src', '') if img_tag else ""
                if url_image and url_image.startswith('/'):
                    url_image = f"https://robertsspaceindustries.com{url_image}"
                # --- End Extract Image URL ---

                name_wrap = card.find("span", class_="name-wrap")
                if not name_wrap:
                    continue

                name_tag = name_wrap.find("span", class_="name")
                handle_tag = name_wrap.find("span", class_="nick")
                rank_tag = card.find("span", class_="rank")

                name = name_tag.text.replace('\u00a0', ' ').strip() if name_tag else "UNKNOWN_NAME"
                handle = handle_tag.text.replace('\u00a0', ' ').strip() if handle_tag else "UNKNOWN_HANDLE"
                rank = rank_tag.text.replace('\u00a0', ' ').strip() if rank_tag else "N/A"

                if not name or name.isspace() or not handle or handle.isspace() or handle == "UNKNOWN_HANDLE":
                    continue

                member = Member(
                    name=name,
                    symbol=handle, # Handle is the symbol
                    url_image=url_image,
                    url_member=url_member,
                    rank=rank
                )
                members.append(member)

            except Exception as e:
                logger.error(f"Error parsing a member card for org '{org_symbol}': {e}. Card HTML: {item}", exc_info=True) # Added org symbol here too
                continue # Continue with the next card

        return members


class OrganizationImporter:
    """Main class for importing Star Citizen organizations."""

    def __init__(self, db_path: str = 'sc_organizations.db'):
        """Initializes the importer.

        Args:
            db_path: Path to the SQLite database.
        """
        self.db_path = db_path
        self.db_manager = DatabaseManager(db_path)

    async def setup(self) -> None:
        """Configures the importer."""
        self.db_manager.connect()
        self.db_manager.setup_database()

    async def migrate_from_old_db(self, old_db_path: str) -> None:
        """Migrates data from the old database.

        Args:
            old_db_path: Path to the old database.
        """
        logger.info(f"Migrating from {old_db_path} to {self.db_path}")
        self.db_manager.migrate_from_old_db(old_db_path)

    async def import_organizations(self, session: aiohttp.ClientSession, sort_methods: List[str] = None) -> None:
        """Imports organizations from the RSI API.

        Args:
            session: aiohttp session to use for requests.
            sort_methods: Sorting methods to use for retrieving organizations.
        """
        if sort_methods is None:
            sort_methods = ["created_desc", "created_asc", "size_desc", "size_asc", "active_desc", "active_asc"]

        api_client = RSIApiClient(session)

        for sort in sort_methods:
            logger.info(f"Importing organizations with sort: {sort}")

            for page in tqdm(range(1, 401), desc=f"Importing organizations ({sort})"):
                orgs = await api_client.get_organizations(page=page, sort=sort)

                if not orgs:
                    logger.info(f"No organizations found for page {page} with sort {sort}")
                    break

                for org in orgs:
                    try:
                        self.db_manager.save_organization(org)
                    except Exception as e:
                        logger.error(f"Error saving organization {org.symbol}: {e}")

    async def update_existing_organizations(self, session: aiohttp.ClientSession, hours: int = 1) -> None:
        """Updates existing organizations.

        Args:
            session: aiohttp session to use for requests.
            hours: Number of hours since the last update.
        """
        api_client = RSIApiClient(session)
        orgs_to_update = self.db_manager.get_organizations_to_update(hours)

        logger.info(f"Updating {len(orgs_to_update)} organizations")

        for org_data in tqdm(orgs_to_update, desc="Updating organizations"):
            orgs = await api_client.get_organizations(page=1, search=org_data['symbol'])

            if orgs and len(orgs) > 0:
                try:
                    self.db_manager.save_organization(orgs[0])
                except Exception as e:
                    logger.error(f"Error updating organization {org_data['symbol']}: {e}")

    async def import_members(self, session: aiohttp.ClientSession) -> None:
        """Imports members of organizations using optimized batch processing."""
        api_client = RSIApiClient(session)
        orgs_for_members = self.db_manager.get_organizations_for_member_update()

        logger.info(f"Optimized member import starting for {len(orgs_for_members)} organizations")

        # Pre-fetch all existing member symbols and IDs once (can consume memory for large member tables)
        # Consider fetching within the loop if memory becomes an issue
        try:
            all_member_symbols = self.db_manager.get_all_member_symbols()
            logger.info(f"Pre-fetched {len(all_member_symbols)} existing member symbols.")
        except Exception as e:
            logger.error(f"Failed to pre-fetch member symbols: {e}. Aborting member import.")
            return

        for org_data in tqdm(orgs_for_members, desc="Importing members (Optimized)"):
            org_id = org_data['id']
            org_symbol = org_data['symbol']
            logger.debug(f"Processing members for {org_symbol} (ID: {org_id})")

            try:
                # Pre-fetch current active members and ranks for THIS organization
                active_org_members = self.db_manager.get_active_org_members_with_rank(org_id)
                logger.debug(f"Pre-fetched {len(active_org_members)} active members for {org_symbol}.")

                # Fetch members from API
                result = await api_client.get_organization_members(org_symbol)
                if not result:
                    logger.warning(f"Could not retrieve members for {org_symbol} from API. Skipping.")
                    continue

                api_members, total = result
                logger.info(f"Organization {org_symbol}: {len(api_members)}/{total} members retrieved from API")

                # --- Prepare lists for batch operations --- #
                members_to_insert = [] # List of Member objects
                members_to_update = [] # List of Member objects with existing ID
                associations_to_insert = [] # List of tuples (member_id, org_id, rank)
                associations_to_update = [] # List of tuples (rank, association_id_to_update)
                rank_history_to_insert = [] # List of tuples (member_id, org_id, rank)
                retrieved_member_symbols = set()
                processed_member_ids = set()

                # --- Process members retrieved from API --- #
                for api_member in api_members:
                    retrieved_member_symbols.add(api_member.symbol)
                    member_id = None
                    is_new_member = False

                    # Check if member exists in our global list
                    if api_member.symbol in all_member_symbols:
                        member_id = all_member_symbols[api_member.symbol]
                        api_member.id = member_id # Assign existing ID
                        members_to_update.append(api_member)
                        processed_member_ids.add(member_id)
                    else:
                        # Member is new to the database
                        is_new_member = True
                        members_to_insert.append(api_member)
                        # We need the ID after insertion for association, handle this later

                    # Check association and rank (only if member ID is known)
                    if member_id is not None:
                        association_data = active_org_members.get(member_id)

                        if association_data is None:
                            # Member exists but wasn't active in this org -> new association
                            associations_to_insert.append((member_id, org_id, api_member.rank))
                            rank_history_to_insert.append((member_id, org_id, api_member.rank))
                        else:
                            current_rank, association_id = association_data
                            if current_rank != api_member.rank:
                                # Member is active, rank changed
                                logger.info(f"Rank change for {api_member.symbol} in {org_symbol}: {current_rank} -> {api_member.rank}")
                                associations_to_update.append((api_member.rank, association_id)) # Use the fetched association_id
                                rank_history_to_insert.append((member_id, org_id, api_member.rank))
                            # else: Member active, rank unchanged - do nothing for association/history

                # --- Determine Departures --- #
                departing_member_ids = []
                for member_id, (rank, assoc_id) in active_org_members.items(): # Unpack tuple here
                    if member_id not in processed_member_ids:
                        departing_member_ids.append(member_id)
                        # Optionally log departure here
                        # logger.info(f"Member ID {member_id} left {org_symbol}")

                # --- Perform Batch DB Operations within a Transaction --- #
                try:
                    self.db_manager.connection.execute("BEGIN TRANSACTION")
                    logger.debug(f"Beginning transaction for {org_symbol}")

                    # 1. Batch insert new members
                    newly_inserted_ids = self.db_manager.batch_insert_members(members_to_insert)
                    if newly_inserted_ids:
                        # Update global symbol map
                        all_member_symbols.update(newly_inserted_ids)
                        # Prepare associations and history for newly inserted members
                        for member_obj in members_to_insert:
                            new_id = newly_inserted_ids.get(member_obj.symbol)
                            if new_id:
                                associations_to_insert.append((new_id, org_id, member_obj.rank))
                                rank_history_to_insert.append((new_id, org_id, member_obj.rank))
                            else:
                                logger.warning(f"Could not find newly inserted ID for symbol {member_obj.symbol}")

                    # 2. Batch update existing members
                    self.db_manager.batch_update_members(members_to_update)

                    # 3. Batch insert new associations (now includes those for new members)
                    self.db_manager.batch_insert_associations(associations_to_insert)

                    # 4. Batch update existing associations rank
                    self.db_manager.batch_update_associations_rank(associations_to_update)

                    # 5. Batch insert rank history (now includes those for new members)
                    self.db_manager.batch_insert_rank_history(rank_history_to_insert)

                    # 6. Batch mark departures
                    self.db_manager.batch_mark_departures(departing_member_ids, org_id)

                    # 7. Mark organization members as updated
                    self.db_manager.mark_organization_members_updated(org_id)

                    # 8. Commit Transaction
                    self.db_manager.connection.commit()
                    logger.debug(f"Committed transaction for {org_symbol}")

                except Exception as db_error:
                    logger.error(f"Database error during batch processing for {org_symbol}: {db_error}", exc_info=True)
                    self.db_manager.connection.rollback()
                    logger.warning(f"Rolled back transaction for {org_symbol}")
                    # Do not skip the org, just log the error and continue to the next

                # --- End Batch DB Operations --- #

            except Exception as e:
                logger.error(f"Error importing members for {org_symbol}: {e}", exc_info=True)
                # No transaction to rollback here as it's handled in the inner try/except

        logger.info("Finished optimized member import cycle.")

    async def run_import_cycle(self) -> None:
        """Runs a full import cycle."""
        async with aiohttp.ClientSession() as session:
            logger.info("Starting import cycle")

            try:
                # Import organizations with different sort methods
                await self.import_organizations(session)

                # Update existing organizations
                await self.update_existing_organizations(session)

                # Import members
                await self.import_members(session)

                # Reset the member update flag
                self.db_manager.reset_members_updated_flag()

                logger.info("Import cycle finished")

            except Exception as e:
                logger.error(f"Error during import cycle: {e}")

    async def run_continuous_import(self, interval_seconds: int = 14400) -> None:
        """Runs the import continuously at regular intervals.

        Args:
            interval_seconds: Interval in seconds between import cycles.
        """
        while True:
            try:
                await self.run_import_cycle()
                logger.info(f"Waiting for {interval_seconds // 3600} hours before the next import cycle")
                await asyncio.sleep(interval_seconds)
            except Exception as e:
                logger.error(f"Critical error in continuous import loop: {e}")
                await asyncio.sleep(300)  # Wait 5 minutes before retrying on critical error


async def main() -> None:
    """Main function."""
    # Configure logger
    logging.basicConfig(
        filename='sc_org_importer.log',
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger("SCOrgImporter")

    try:
        # Initialize importer
        importer = OrganizationImporter('sc_organizations.db')
        await importer.setup()

        # Run continuous import
        await importer.run_continuous_import()

    except Exception as e:
        logger.critical(f"Fatal error in main function: {e}")
        import traceback
        logger.critical(traceback.format_exc())


if __name__ == "__main__":
    import os
    # Run the main coroutine
    asyncio.run(main())