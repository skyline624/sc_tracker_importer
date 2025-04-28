#!/bin/bash

# Script de démarrage pour l'importateur d'organisations Star Citizen
echo "=== Démarrage de l'importateur d'organisations SC ==="
echo "Répertoire courant: $(pwd)"

# Vérifier si Python est installé
if ! command -v python3 &> /dev/null; then
    echo "Python3 n'est pas installé. Veuillez l'installer."
    exit 1
fi

# Définir le chemin du répertoire virtuel
VENV_DIR=".venv"

# Créer l'environnement virtuel s'il n'existe pas
if [ ! -d "$VENV_DIR" ]; then
    echo "Création de l'environnement virtuel Python..."
    python3 -m venv $VENV_DIR
    if [ $? -ne 0 ]; then
        echo "Erreur lors de la création de l'environnement virtuel."
        exit 1
    fi
    echo "Environnement virtuel créé avec succès."
else
    echo "Environnement virtuel existant détecté."
fi

# Activer l'environnement virtuel
source $VENV_DIR/bin/activate
if [ $? -ne 0 ]; then
    echo "Erreur lors de l'activation de l'environnement virtuel."
    exit 1
fi
echo "Environnement virtuel activé avec succès."

# Utiliser le fichier requirements.txt spécifique à l'importateur
REQ_FILE="requirements.txt"

# Vérifier si le fichier requirements.txt existe
if [ ! -f "$REQ_FILE" ]; then
    echo "Le fichier $REQ_FILE n'a pas été trouvé."
    exit 1
fi

# Installer les dépendances
echo "Installation des dépendances depuis $REQ_FILE..."
pip install -r $REQ_FILE
if [ $? -ne 0 ]; then
    echo "Erreur lors de l'installation des dépendances."
    exit 1
fi

echo "Toutes les dépendances ont été installées avec succès."

# Lancer le programme
echo "=== Démarrage de l'importateur d'organisations SC ==="
echo "Appuyez sur CTRL+C pour arrêter l'importateur"
echo "==============================================="

python3 org_importer.py

# Désactiver l'environnement virtuel à la fin
deactivate