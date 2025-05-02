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
from typing import Dict, List, Optional, Tuple, Union, Any, Set
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
        """Establishes a connection to the database and sets pragmas."""
        self.connection = sqlite3.connect(self.db_path, timeout=10) # Increased timeout
        self.connection.row_factory = sqlite3.Row
        self.cursor = self.connection.cursor()
        # Performance Pragmas
        self.cursor.execute("PRAGMA journal_mode=WAL;")
        self.cursor.execute("PRAGMA synchronous=NORMAL;")
        self.cursor.execute("PRAGMA cache_size = -10000;") # Cache ~10MB
        self.cursor.execute("PRAGMA temp_store = MEMORY;")
        self.cursor.execute("PRAGMA busy_timeout = 5000;") # Wait 5s if locked

    def disconnect(self) -> None:
        """Closes the database connection."""
        if self.connection:
            # Ensure WAL checkpoint before closing
            try:
                self.cursor.execute("PRAGMA wal_checkpoint(TRUNCATE);")
            except sqlite3.Error as e:
                logger.warning(f"Could not perform WAL checkpoint: {e}")
            finally:
                if self.cursor:
                    self.cursor.close()
                    self.cursor = None
                if self.connection:
                    self.connection.close()
                    self.connection = None

    def commit(self):
        """Commits the current transaction."""
        if self.connection:
            try:
                self.connection.commit()
            except sqlite3.Error as e:
                logger.error(f"Error during commit: {e}")
                self.connection.rollback()
                raise

    def rollback(self):
        """Rolls back the current transaction."""
        if self.connection:
            self.connection.rollback()

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

        # Indices for performance improvement (ensure they are created)
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_org_symbol ON organizations (symbol)")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_member_symbol ON members (symbol)")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_member_org ON member_organization (member_id, organization_id)")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_member_org_active ON member_organization (member_id, organization_id, is_active)") # Added for faster active checks
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_org_history ON organization_history (organization_id, timestamp)") # Added for history lookups
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_rank_history ON member_rank_history (member_id, organization_id, timestamp)") # Added for history lookups
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_org_last_updated ON organizations (is_active, last_updated)") # For get_organizations_to_update
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_org_members_updated ON organizations (is_active, members_updated)") # For get_organizations_for_member_update

        self.commit() # Commit after setup

    def migrate_from_old_db(self, old_db_path: str) -> None:
        """Migrates data from the old database structure using batching."""
        # NOTE: For brevity, the migration logic is not fully rewritten here for batching.
        # The principles would be:
        # 1. Read data in larger chunks from the old DB.
        # 2. Prepare lists of tuples for batch inserts/updates.
        # 3. Use executemany for inserts into new tables.
        # 4. Commit periodically or at the end.
        # This part requires careful handling of IDs and relationships.
        # Keeping the original migration logic for now, assuming it's a one-time operation.
        # If migration speed is critical, it should be refactored similarly to the import methods below.
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

            self.commit()
            logger.info("Migration completed successfully!")

        except sqlite3.Error as e:
            logger.error(f"Error during migration: {e}")
            self.rollback()
            raise
        finally:
            if 'old_conn' in locals():
                old_conn.close()

    def save_organizations_batch(self, orgs: List[Organization]) -> Dict[str, int]:
        """Saves a batch of organizations, handling updates and history."""
        org_ids = {}
        if not orgs:
            return org_ids

        symbols = tuple(org.symbol for org in orgs)
        placeholders = ','.join('?' * len(symbols))

        # Fetch existing orgs in this batch
        self.cursor.execute(f"SELECT * FROM organizations WHERE symbol IN ({placeholders})", symbols)
        existing_orgs_dict = {row['symbol']: dict(row) for row in self.cursor.fetchall()}

        new_orgs_data = []
        update_orgs_data = []
        history_data = []

        for org in orgs:
            existing_org = existing_orgs_dict.get(org.symbol)
            is_new = existing_org is None
            changes = []

            if is_new:
                new_orgs_data.append((
                    org.name, org.symbol, org.url_image, org.url_corpo, org.archetype,
                    org.langage, org.commitment, org.recrutement, org.role_play, org.nb_membres
                ))
                # Placeholder for ID, will be filled after insert
                org_ids[org.symbol] = -1 # Mark as new
            else:
                org_id = existing_org['id']
                org_ids[org.symbol] = org_id # Store existing ID

                # Compare attributes (similar to original save_organization)
                if org.name != existing_org['name']: changes.append(f"Name: {existing_org['name']} -> {org.name}")
                if org.url_image != existing_org['url_image']: changes.append(f"Image URL: {existing_org['url_image']} -> {org.url_image}")
                # ... (add all other field comparisons) ...
                if org.nb_membres != existing_org['member_count']: changes.append(f"Member count: {existing_org['member_count']} -> {org.nb_membres}")

                if changes:
                    change_desc = ", ".join(changes)
                    update_orgs_data.append((
                        org.name, org.url_image, org.url_corpo, org.archetype,
                        org.langage, org.commitment, org.recrutement, org.role_play,
                        org.nb_membres, org_id
                    ))
                    history_data.append((
                        org_id, org.name, org.symbol, org.url_image, org.url_corpo,
                        org.archetype, org.langage, org.commitment, org.recrutement,
                        org.role_play, org.nb_membres, change_desc
                    ))
                else:
                    # No data change, but update timestamp
                    update_orgs_data.append((
                        existing_org['name'], existing_org['url_image'], existing_org['url_corpo'], existing_org['archetype'],
                        existing_org['language'], existing_org['commitment'], existing_org['recruitment'], existing_org['role_play'],
                        existing_org['member_count'], org_id
                    )) # Update with existing data just to touch timestamp

        try:
            # Insert new organizations
            if new_orgs_data:
                self.cursor.executemany("""
                    INSERT INTO organizations (
                        name, symbol, url_image, url_corpo, archetype,
                        language, commitment, recruitment, role_play, member_count
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, new_orgs_data)

                # Retrieve IDs for newly inserted orgs and add history entries
                new_symbols = tuple(org[1] for org in new_orgs_data)
                placeholders_new = ','.join('?' * len(new_symbols))
                self.cursor.execute(f"SELECT id, symbol FROM organizations WHERE symbol IN ({placeholders_new})", new_symbols)
                newly_inserted_ids = {row['symbol']: row['id'] for row in self.cursor.fetchall()}

                for org_data in new_orgs_data:
                    symbol = org_data[1]
                    new_id = newly_inserted_ids.get(symbol)
                    if new_id:
                        org_ids[symbol] = new_id # Update placeholder ID
                        history_data.append((
                            new_id, org_data[0], org_data[1], org_data[2], org_data[3],
                            org_data[4], org_data[5], org_data[6], org_data[7],
                            org_data[8], org_data[9], "Organization created"
                        ))

            # Update existing organizations
            if update_orgs_data:
                self.cursor.executemany("""
                    UPDATE organizations SET
                    name = ?, url_image = ?, url_corpo = ?, archetype = ?,
                    language = ?, commitment = ?, recruitment = ?, role_play = ?,
                    member_count = ?, last_updated = CURRENT_TIMESTAMP
                    WHERE id = ?
                """, update_orgs_data)

            # Insert history records
            if history_data:
                self.cursor.executemany("""
                    INSERT INTO organization_history (
                        organization_id, name, symbol, url_image, url_corpo,
                        archetype, language, commitment, recruitment, role_play,
                        member_count, change_description
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, history_data)

            # Update last_updated for orgs without changes (already included in update_orgs_data)

            # No commit here, handled by caller
            return org_ids

        except sqlite3.Error as e:
            logger.error(f"Error batch saving organizations: {e}")
            self.rollback() # Rollback this batch on error
            raise

    def save_members_batch(self, members: List[Member]) -> Dict[str, int]:
        """Saves a batch of members using INSERT OR IGNORE and UPDATE."""
        member_ids = {}
        if not members:
            return member_ids

        insert_data = []
        update_data = []
        symbols = set() # Track symbols in this batch

        for member in members:
            if member.symbol in symbols: continue # Avoid duplicate processing within batch
            symbols.add(member.symbol)
            insert_data.append((member.name, member.symbol, member.url_image, member.url_member))
            update_data.append((member.name, member.url_image, member.url_member, member.symbol))

        try:
            # Attempt to insert all members, ignoring duplicates based on symbol UNIQUE constraint
            self.cursor.executemany("""
                INSERT OR IGNORE INTO members (name, symbol, url_image, url_member)
                VALUES (?, ?, ?, ?)
            """, insert_data)

            # Update existing members (or newly inserted ones) to ensure latest data
            self.cursor.executemany("""
                UPDATE members SET
                name = ?, url_image = ?, url_member = ?, last_updated = CURRENT_TIMESTAMP
                WHERE symbol = ?
            """, update_data)

            # Retrieve IDs for all members in the batch
            member_symbols = tuple(m.symbol for m in members)
            placeholders = ','.join('?' * len(member_symbols))
            self.cursor.execute(f"SELECT id, symbol FROM members WHERE symbol IN ({placeholders})", member_symbols)
            member_ids = {row['symbol']: row['id'] for row in self.cursor.fetchall()}

            # No commit here
            return member_ids

        except sqlite3.Error as e:
            logger.error(f"Error batch saving members: {e}")
            self.rollback()
            raise

    def save_member_associations_batch(self, associations: List[Tuple[int, int, str]]) -> None:
        """Saves member-organization associations in batch, handling rank changes."""
        if not associations:
            return

        # Prepare data for checking existing active associations
        unique_pairs = list({(assoc[0], assoc[1]) for assoc in associations})
        member_ids = tuple(p[0] for p in unique_pairs)
        org_ids = tuple(p[1] for p in unique_pairs)

        # Build query dynamically - might be very long for large batches
        # Consider fetching in smaller chunks if needed
        if not unique_pairs: return

        query = """
            SELECT member_id, organization_id, id, rank
            FROM member_organization
            WHERE is_active = 1 AND ("""
        query += " OR ".join(["(member_id = ? AND organization_id = ?)"] * len(unique_pairs))
        query += ")"
        params = [item for pair in unique_pairs for item in pair]

        self.cursor.execute(query, params)
        existing_assoc_dict = {(row['member_id'], row['organization_id']): {'id': row['id'], 'rank': row['rank']}
                               for row in self.cursor.fetchall()}

        insert_assoc_data = []
        update_rank_data = []
        rank_history_data = []

        processed_assoc = set() # Track processed (member_id, org_id) pairs

        for member_id, org_id, rank in associations:
            assoc_key = (member_id, org_id)
            if assoc_key in processed_assoc: continue # Process each pair only once per batch
            processed_assoc.add(assoc_key)

            existing = existing_assoc_dict.get(assoc_key)

            if existing:
                # Association exists, check for rank change
                if existing['rank'] != rank:
                    logger.info(f"Rank change detected for member {member_id} in org {org_id}: {existing['rank']} -> {rank}")
                    update_rank_data.append((rank, existing['id']))
                    rank_history_data.append((member_id, org_id, rank))
            else:
                # New association
                insert_assoc_data.append((member_id, org_id, rank))
                rank_history_data.append((member_id, org_id, rank))

        try:
            # Insert new associations
            if insert_assoc_data:
                self.cursor.executemany("""
                    INSERT OR IGNORE INTO member_organization (member_id, organization_id, rank)
                    VALUES (?, ?, ?)
                """, insert_assoc_data) # Use IGNORE in case it was inserted but not active

            # Update ranks for existing associations
            if update_rank_data:
                self.cursor.executemany("""
                    UPDATE member_organization SET rank = ? WHERE id = ?
                """, update_rank_data)

            # Insert rank history
            if rank_history_data:
                self.cursor.executemany("""
                    INSERT INTO member_rank_history (member_id, organization_id, rank)
                    VALUES (?, ?, ?)
                """, rank_history_data)

            # No commit here

        except sqlite3.Error as e:
            logger.error(f"Error batch saving member associations: {e}")
            self.rollback()
            raise

    def mark_members_left_batch(self, departures: List[Tuple[int, int]]) -> None:
        """Marks multiple members as having left their organizations."""
        if not departures:
            return

        # Filter out duplicates within the batch
        unique_departures = list({tuple(dep) for dep in departures})

        update_data = [(0, datetime.now(), mid, oid) for mid, oid in unique_departures]

        try:
            # Update active entries to inactive
            self.cursor.executemany("""
                UPDATE member_organization SET
                is_active = ?, left_at = ?
                WHERE member_id = ? AND organization_id = ? AND is_active = 1
            """, update_data)

            # Log how many were actually updated
            if self.cursor.rowcount < len(unique_departures):
                 logger.debug(f"Marked {self.cursor.rowcount} members as left out of {len(unique_departures)} requested (some might have already left or never existed).")
            # No commit here

        except sqlite3.Error as e:
            logger.error(f"Error batch marking members left: {e}")
            self.rollback()
            raise

    def mark_organizations_updated_batch(self, org_ids: List[int]) -> None:
        """Marks multiple organizations as having their members updated."""
        if not org_ids:
            return

        unique_org_ids = list(set(org_ids))
        update_data = [(1, org_id) for org_id in unique_org_ids]

        try:
            self.cursor.executemany("""
                UPDATE organizations SET members_updated = ? WHERE id = ?
            """, update_data)
            # No commit here

        except sqlite3.Error as e:
            logger.error(f"Error batch marking organizations updated: {e}")
            self.rollback()
            raise

    # --- Methods that don't need batching or are read-only ---

    def reset_members_updated_flag(self) -> None:
        """Resets the members_updated flag for all organizations."""
        try:
            self.cursor.execute("UPDATE organizations SET members_updated = 0 WHERE members_updated = 1") # Only update if needed
            self.commit() # Commit this change
        except sqlite3.Error as e:
            logger.error(f"Error resetting members_updated flags: {e}")
            self.rollback()
            raise

    def get_organizations_to_update(self, hours: int = 1) -> List[Dict[str, Any]]:
        """Retrieves organizations that haven't been updated in a while."""
        try:
            # Use the index idx_org_last_updated
            self.cursor.execute("""
                SELECT id, symbol FROM organizations
                WHERE is_active = 1 AND last_updated <= datetime('now', ?)
            """, (f"-{hours} hours",)) # Corrected parameter binding
            return [dict(row) for row in self.cursor.fetchall()]
        except sqlite3.Error as e:
            logger.error(f"Error retrieving organizations to update: {e}")
            raise # Re-raise read errors

    def get_organizations_for_member_update(self) -> List[Dict[str, Any]]:
        """Retrieves organizations whose members need to be updated."""
        try:
            # Use the index idx_org_members_updated
            self.cursor.execute("""
                SELECT id, symbol FROM organizations
                WHERE is_active = 1 AND members_updated = 0
                ORDER BY id ASC
            """)
            return [dict(row) for row in self.cursor.fetchall()]
        except sqlite3.Error as e:
            logger.error(f"Error retrieving organizations for member update: {e}")
            raise

    def get_current_members(self, org_id: int) -> Dict[str, int]:
        """Retrieves the current members of an organization."""
        try:
            # Use index idx_member_org_active
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

    def get_member_name(self, member_id: int) -> str:
        """Retrieves a member's name from their ID."""
        try:
            self.cursor.execute("SELECT name FROM members WHERE id = ?", (member_id,)) # Use index on members.id (PK)
            result = self.cursor.fetchone()
            return result['name'] if result else ""
        except sqlite3.Error as e:
            logger.error(f"Error retrieving name for member {member_id}: {e}")
            return "" # Return empty on error, don't raise

    # --- Remove single save/update methods if no longer needed, or keep for specific cases ---
    # def save_organization(self, org: Organization) -> Tuple[int, bool]: ...
    # def save_member(self, member: Member) -> int: ...
    # def save_member_organization(self, member_id: int, org_id: int, rank: str) -> None: ...
    # def mark_member_left_organization(self, member_id: int, org_id: int) -> None: ...
    # def mark_organization_members_updated(self, org_id: int) -> None: ...


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

        members = await self._parse_members_page(response['data']['html'])

        # Retrieve additional pages
        for page in range(2, total_pages + 1):
            data['page'] = page
            page_response = await self._make_request("orgs/getOrgMembers", data)

            if not page_response or 'data' not in page_response or 'html' not in page_response['data']:
                logger.error(f"Error retrieving page {page} of members")
                continue

            page_members = await self._parse_members_page(page_response['data']['html'])
            members.extend(page_members)

        return (members, total_rows)

    async def _parse_members_page(self, html: str) -> List[Member]:
        """Parses an HTML page of members.

        Args:
            html: HTML content to parse.

        Returns:
            List of members extracted from the page.
        """
        members = []
        soup = BeautifulSoup(html, "lxml")
        member_cards = soup.find_all("a", {"class": "membercard js-edit-member"})

        for card in member_cards:
            try:
                url_member = card.get('href', "")
                if url_member:
                    url_member = f"https://robertsspaceindustries.com{url_member}"

                img_tag = card.find("img")
                img_member = img_tag['src'] if img_tag else ""

                name_wrap = card.find("span", {"class": "name-wrap"})
                if not name_wrap:
                    continue

                name_wrap_soup = BeautifulSoup(str(name_wrap), "lxml")
                spans = name_wrap_soup.find_all("span")

                if len(spans) < 3:
                    continue

                name = spans[1].text
                handle = spans[2].text

                rank_tag = card.find("span", {"class": "rank"})
                rank = rank_tag.text if rank_tag else "N/A"

                member = Member(
                    name=name,
                    symbol=handle,
                    url_image=img_member,
                    url_member=url_member,
                    rank=rank
                )

                members.append(member)

            except Exception as e:
                logger.error(f"Error parsing a member card: {e}")
                continue

        return members


class OrganizationImporter:
    """Main class for importing Star Citizen organizations."""

    def __init__(self, db_path: str = 'sc_organizations.db', batch_size: int = 100):
        """Initializes the importer.

        Args:
            db_path: Path to the SQLite database.
            batch_size: Number of items to process before committing.
        """
        self.db_path = db_path
        self.db_manager = DatabaseManager(db_path)
        self.batch_size = batch_size # Define a batch size

    async def setup(self) -> None:
        """Configures the importer."""
        self.db_manager.connect()
        self.db_manager.setup_database()
        # No disconnect here, managed by run methods

    async def close(self) -> None:
        """Closes database connection."""
        self.db_manager.disconnect()

    async def migrate_from_old_db(self, old_db_path: str) -> None:
        """Migrates data from the old database.

        Args:
            old_db_path: Path to the old database.
        """
        logger.info(f"Migrating from {old_db_path} to {self.db_path}")
        # Assuming migration is one-off and might not need extreme batching
        # If it's slow, it needs refactoring similar to import methods
        self.db_manager.migrate_from_old_db(old_db_path)

    async def import_organizations(self, session: aiohttp.ClientSession, sort_methods: List[str] = None) -> None:
        """Imports organizations from the RSI API using batching."""
        if sort_methods is None:
            sort_methods = ["created_desc", "created_asc", "size_desc", "size_asc", "active_desc", "active_asc"]

        api_client = RSIApiClient(session)
        org_batch = []

        for sort in sort_methods:
            logger.info(f"Importing organizations with sort: {sort}")
            page_num = 1
            while page_num <= 400: # Limit pages or use a different stop condition
                orgs = await api_client.get_organizations(page=page_num, sort=sort)

                if not orgs:
                    logger.info(f"No more organizations found for page {page_num} with sort {sort}")
                    break # Stop for this sort method

                org_batch.extend(orgs)

                if len(org_batch) >= self.batch_size:
                    try:
                        logger.info(f"Saving batch of {len(org_batch)} organizations...")
                        with self.db_manager.connection: # Use transaction context manager
                             self.db_manager.save_organizations_batch(org_batch)
                        # Commit happens automatically on exit of 'with' block if no exception
                        org_batch = [] # Clear batch
                    except Exception as e:
                        logger.error(f"Error saving organization batch: {e}")
                        # Consider how to handle batch errors (skip batch, retry?)
                        org_batch = [] # Clear batch even on error to avoid retrying same data

                page_num += 1
                await asyncio.sleep(0.1) # Small delay between pages

            # Save any remaining orgs in the batch
            if org_batch:
                try:
                    logger.info(f"Saving final batch of {len(org_batch)} organizations...")
                    with self.db_manager.connection:
                        self.db_manager.save_organizations_batch(org_batch)
                    org_batch = []
                except Exception as e:
                    logger.error(f"Error saving final organization batch: {e}")
                    org_batch = []


    async def update_existing_organizations(self, session: aiohttp.ClientSession, hours: int = 1) -> None:
        """Updates existing organizations using batching."""
        api_client = RSIApiClient(session)
        orgs_to_update = self.db_manager.get_organizations_to_update(hours)
        logger.info(f"Found {len(orgs_to_update)} organizations potentially needing update (older than {hours}h)")

        org_batch_to_save = []

        for i, org_data in enumerate(tqdm(orgs_to_update, desc="Checking organizations for updates")):
            # Fetch details for one org at a time using search
            # Batching fetches here is harder as API doesn't support multiple symbols
            orgs = await api_client.get_organizations(page=1, search=org_data['symbol'])

            if orgs and len(orgs) > 0:
                # Check if the found org matches the symbol exactly
                if orgs[0].symbol == org_data['symbol']:
                    org_batch_to_save.append(orgs[0])

            # Save in batches
            if len(org_batch_to_save) >= self.batch_size or (i == len(orgs_to_update) - 1 and org_batch_to_save):
                try:
                    logger.info(f"Saving batch of {len(org_batch_to_save)} updated organizations...")
                    with self.db_manager.connection:
                        self.db_manager.save_organizations_batch(org_batch_to_save)
                    org_batch_to_save = [] # Clear batch
                except Exception as e:
                    logger.error(f"Error saving updated organization batch: {e}")
                    org_batch_to_save = []

            await asyncio.sleep(0.1) # Small delay between checks


    async def import_members(self, session: aiohttp.ClientSession) -> None:
        """Imports members of organizations using batching."""
        api_client = RSIApiClient(session)
        orgs_for_members = self.db_manager.get_organizations_for_member_update()
        logger.info(f"Found {len(orgs_for_members)} organizations needing member import")

        org_ids_updated = []

        for i, org_data in enumerate(tqdm(orgs_for_members, desc="Importing members")):
            org_id = org_data['id']
            symbol = org_data['symbol']
            logger.debug(f"Processing members for organization {symbol} (ID: {org_id})")

            try:
                # Fetch all members for the organization
                result = await api_client.get_organization_members(symbol)
                if not result:
                    logger.warning(f"Could not retrieve members for {symbol}, skipping.")
                    continue # Skip this org if members can't be fetched

                fetched_members_list, total_api_count = result
                logger.info(f"Organization {symbol}: Retrieved {len(fetched_members_list)} members (API reports {total_api_count})")

                if not fetched_members_list:
                     # Mark as updated even if empty, to avoid re-checking immediately
                    org_ids_updated.append(org_id)
                    logger.info(f"Organization {symbol} has no members according to API.")
                    # Commit periodically or at the end
                    if len(org_ids_updated) >= self.batch_size or i == len(orgs_for_members) - 1:
                         if org_ids_updated:
                            with self.db_manager.connection:
                                self.db_manager.mark_organizations_updated_batch(org_ids_updated)
                            org_ids_updated = []
                    continue # Go to next org

                # Use transaction for all operations related to this single organization
                with self.db_manager.connection:
                    # 1. Get current members from DB
                    current_db_members = self.db_manager.get_current_members(org_id) # {symbol: member_id}

                    # 2. Save fetched members (batch) and get their IDs
                    member_symbol_to_id = self.db_manager.save_members_batch(fetched_members_list)

                    # 3. Prepare associations and departures
                    associations_to_save = []
                    departures_to_mark = []
                    retrieved_member_symbols = set()

                    for member in fetched_members_list:
                        member_id = member_symbol_to_id.get(member.symbol)
                        if member_id:
                            associations_to_save.append((member_id, org_id, member.rank))
                            retrieved_member_symbols.add(member.symbol)
                        else:
                            logger.warning(f"Could not find ID for member {member.symbol} after batch save.")

                    # 4. Find members who left (in DB but not in fetched list)
                    db_symbols = set(current_db_members.keys())
                    left_symbols = db_symbols - retrieved_member_symbols

                    for symbol in left_symbols:
                        member_id = current_db_members[symbol]
                        member_name = self.db_manager.get_member_name(member_id) # Read name (ok in transaction)
                        logger.info(f"Member '{member_name}' (ID: {member_id}, Symbol: {symbol}) left organization {symbol}")
                        departures_to_mark.append((member_id, org_id))

                    # 5. Save associations (batch)
                    self.db_manager.save_member_associations_batch(associations_to_save)

                    # 6. Mark departures (batch)
                    self.db_manager.mark_members_left_batch(departures_to_mark)

                    # 7. Mark organization as updated (add to list for batch update later)
                    org_ids_updated.append(org_id)

                # Commit for this organization happens automatically via 'with' block

            except Exception as e:
                # Log error and continue with the next organization
                logger.error(f"Error processing members for organization {symbol} (ID: {org_id}): {e}")
                # Rollback is handled by the DatabaseManager methods or the 'with' block context manager

            # Batch update the 'members_updated' flag periodically
            if len(org_ids_updated) >= self.batch_size or (i == len(orgs_for_members) - 1 and org_ids_updated):
                try:
                    logger.info(f"Marking batch of {len(org_ids_updated)} organizations as members-updated...")
                    with self.db_manager.connection:
                         self.db_manager.mark_organizations_updated_batch(org_ids_updated)
                    org_ids_updated = [] # Clear batch
                except Exception as e:
                    logger.error(f"Error marking organizations updated batch: {e}")
                    org_ids_updated = []


    async def run_import_cycle(self) -> None:
        """Runs a full import cycle."""
        async with aiohttp.ClientSession() as session:
            logger.info("Starting import cycle")
            start_time = time.time()
            try:
                # Reset the member update flag first (allows re-checking members)
                logger.info("Resetting members_updated flag...")
                self.db_manager.reset_members_updated_flag()

                # Import organizations with different sort methods
                logger.info("Importing/updating organizations...")
                await self.import_organizations(session)

                # Update existing organizations (those not recently checked)
                logger.info("Checking for stale organizations...")
                await self.update_existing_organizations(session, hours=24) # Check orgs older than 24h

                # Import members for organizations marked as needing update
                logger.info("Importing members...")
                await self.import_members(session)

                end_time = time.time()
                logger.info(f"Import cycle finished in {end_time - start_time:.2f} seconds")

            except Exception as e:
                logger.error(f"Error during import cycle: {e}")
                # Ensure rollback if an error escapes the inner logic
                self.db_manager.rollback()


    async def run_continuous_import(self, interval_seconds: int = 14400) -> None:
        """Runs the import continuously at regular intervals."""
        try:
            await self.setup() # Connect DB once
            while True:
                try:
                    await self.run_import_cycle()
                    logger.info(f"Waiting for {interval_seconds // 3600} hours ({interval_seconds}s) before the next import cycle")
                    await asyncio.sleep(interval_seconds)
                except Exception as e:
                    logger.error(f"Critical error in continuous import loop: {e}")
                    logger.info("Attempting to reconnect database and wait before retrying...")
                    await self.close() # Close potentially broken connection
                    await asyncio.sleep(60) # Wait 1 minute
                    await self.setup() # Reconnect
                    await asyncio.sleep(300) # Wait 5 minutes before retrying cycle
        finally:
            await self.close() # Ensure disconnect on exit


async def main() -> None:
    """Main function."""
    # Configure logger (ensure it's configured only once)
    log_file = 'sc_org_importer.log'
    log_level = logging.INFO
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

    # Avoid adding multiple handlers if main is called again
    root_logger = logging.getLogger()
    if not root_logger.hasHandlers():
         logging.basicConfig(filename=log_file, level=log_level, format=log_format)
    else:
        # If handlers exist, ensure file handler is present
        has_file_handler = any(isinstance(h, logging.FileHandler) and h.baseFilename.endswith(log_file) for h in root_logger.handlers)
        if not has_file_handler:
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(logging.Formatter(log_format))
            root_logger.addHandler(file_handler)
        root_logger.setLevel(log_level)


    logger = logging.getLogger("SCOrgImporter") # Get logger instance

    importer = None
    try:
        # Initialize importer
        importer = OrganizationImporter('sc_organizations.db', batch_size=200) # Adjust batch size as needed

        # Optional: Run migration if needed (e.g., based on command-line arg or config)
        # await importer.migrate_from_old_db('corporation_history.db')

        # Run continuous import
        await importer.run_continuous_import()

    except Exception as e:
        logger.critical(f"Fatal error in main function: {e}")
        import traceback
        logger.critical(traceback.format_exc())
    finally:
        if importer:
            logger.info("Closing database connection from main.")
            await importer.close()


if __name__ == "__main__":
    import os
    # Consider platform-specific event loop policies if needed
    # if os.name == 'nt':
    #    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
