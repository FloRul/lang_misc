import concurrent.futures
import requests
from langchain.docstore.document import Document
from langchain_community.document_loaders.base import BaseLoader
from langchain.chains.query_constructor.base import AttributeInfo


class CKanLoader(BaseLoader):
    def __init__(self):
        self._package_ids = get_ckan_package_list()
        print(f"CKanLoader initialized, found {len(self._package_ids)} dataset ids.")
        self.document_content_info = "Les métadonnés du jeux de données se trouvant dans le portail de données ouvertes du gouvernement du Québec."
        self.metadata_field_info = [
            AttributeInfo(
                name="extension_spatiale",
                description="L'extension spatiale de la zone",
                type="string",
            ),
            AttributeInfo(
                name="organisation_principale",
                description="L'organisation principale (structure, établissement)",
                type="string",
            ),
            AttributeInfo(
                name="identifiant_unique",
                description="L'identifiant unique",
                type="string",
            ),
            AttributeInfo(
                name="niveau_acces",
                description="Le niveau d'accès (degré, rang)",
                type="string",
            ),
            AttributeInfo(
                name="localisation_donnees",
                description="La localisation des données (emplacement, situation)",
                type="string",
            ),
            AttributeInfo(
                name="classification_securite",
                description="La classification de sécurité (catégorisation, typologie)",
                type="string",
            ),
            AttributeInfo(
                name="langue",
                description="La langue (idiome, dialecte)",
                type="string",
            ),
            AttributeInfo(
                name="identifiant_licence",
                description="L'identifiant unique de la licence",
                type="string",
            ),
            AttributeInfo(
                name="titre_licence",
                description="Le titre de la licence (nom, appellation)",
                type="string",
            ),
            AttributeInfo(
                name="url_licence",
                description="L'URL de la licence (adresse web, lien)",
                type="string",
            ),
            AttributeInfo(
                name="responsable",
                description="Le mainteneur (responsable, gestionnaire)",
                type="string",
            ),
            AttributeInfo(
                name="email_responsable",
                description="L'email du mainteneur (adresse électronique, courriel)",
                type="string",
            ),
            AttributeInfo(
                name="date_creation_metadata",
                description="La date de création des métadonnées (jour, moment)",
                type="string",
            ),
            AttributeInfo(
                name="date_modification_metadata",
                description="La date de modification des métadonnées (jour, moment)",
                type="string",
            ),
            AttributeInfo(
                name="methodologie",
                description="La méthodologie (procédure, démarche)",
                type="string",
            ),
            AttributeInfo(
                name="nombre_ressources",
                description="Le nombre de ressources (quantité, total)",
                type="integer",
            ),
            AttributeInfo(
                name="nombre_tags",
                description="Le nombre de tags (étiquettes, mots-clés)",
                type="integer",
            ),
            AttributeInfo(
                name="etat",
                description="L'état (statut, condition)",
                type="string",
            ),
            AttributeInfo(
                name="type",
                description="Le type (genre, catégorie)",
                type="string",
            ),
            AttributeInfo(
                name="frequence_mise_a_jour",
                description="La fréquence de mise à jour (périodicité, rythme)",
                type="string",
            ),
            AttributeInfo(
                name="groupes",
                description="Les groupes (ensembles, collections)",
                type="string",
            ),
            AttributeInfo(
                name="tags",
                description="Les tags (étiquettes, mots-clés)",
                type="string",
            ),
            AttributeInfo(
                name="formats_ressources",
                description="Les formats de fichiers des ressources (types, extensions)",
                type="string",
            ),
        ]

    def load(self) -> list[Document]:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(get_ckan_package_details, id)
                for id in self._package_ids
            ]
            docs = [
                create_ckan_doc(future.result())
                for future in concurrent.futures.as_completed(futures)
            ]
        return docs


def create_ckan_doc(data: dict) -> Document:
    content = f"""Le titre (nom, appellation) du jeux de données {data.get('title','_')}.
                Le nom (dénomination, désignation) du jeu de donnée {data.get('name','_')}.
                La description (résumé, présentation) est {data.get('notes','_')}.
                L'auteur (créateur, rédacteur) est {data.get('author','_')}.
                L'email (adresse électronique, courriel, coordonnées) de l'auteur est {data.get('author_email','_')}.
                L'ID (identifiant unique) de l'utilisateur créateur est {data.get('creator_user_id','_')}.
                L'extension (zone, aire) spatiale est {data.get('ext_spatial','_')}.
                L'organisation (structure, établissement) principale est {data.get('extras_organisation_principale','_')}.
                L'ID (identifiant unique) est {data.get("id","_")}.
                Le niveau (degré, rang) d'accès est {get_fr_value('inv_access_level',data.get('inv_access_level','_'))}.
                La localisation (emplacement, situation) des données est {data.get('inv_data_location','_')}.
                La classification (catégorisation, typologie) de sécurité est {get_fr_value('inv_security_classification',data.get('inv_security_classification','_'))}.
                La langue (idiome, dialecte) est {data.get('language','_')}.
                L'ID (identifiant unique) de la licence est {data.get('license_id','_')}.
                Le titre (nom, appellation) de la licence est {data.get('license_title','_')}.
                L'URL (adresse web, lien) de la licence est {data.get('license_url','_')}.
                Le mainteneur (responsable, gestionnaire) est {data.get('maintainer','_')}.
                L'email (adresse électronique, courriel) du mainteneur est {data.get('maintainer_email','_')}.
                La date (jour, moment) de création des métadonnées est {data.get('metadata_created','_')}.
                La date (jour, moment) de modification des métadonnées est {data.get('metadata_modified','_')}.
                La méthodologie (procédure, démarche) est {data.get('methodologie','_')}.
                Le nombre (quantité, total) de ressources est {data.get('num_resources','_')}.
                Le nombre (quantité, total) de tags (étiquettes, mots-clés) est {data.get('num_tags','_')}.
                L'état (statut, condition) est {data.get('state','_')}.
                Le type (genre, catégorie) est {data.get('type','_')}.
                La fréquence (périodicité, rythme) de mise à jour est {get_fr_value('update_frequency',data.get('update_frequency','_'))}.
                Les groupes (ensembles, collections) sont {', '.join([group['display_name'] for group in data.get('groups', [])])}.
                Les tags (étiquettes, mots-clés) sont {', '.join([tag['display_name'] for tag in data.get('tags', [])])}.
                Les formats (types, extensions) de fichiers des ressources sont {', '.join([resource['format'] for resource in data.get('resources', [])])}.
                """

    def filter_metadata(metadata):
        return {k: v for k, v in metadata.items() if v is not None}

    metadata_f = filter_metadata(
        {
            "title": data.get("title", "_"),
            "name": data.get("name", "_"),
            "description": data.get("notes", "_"),
            "extension_spatiale": data.get("ext_spatial", "_"),
            "organisation_principale": data.get("extras_organisation_principale", "_"),
            "identifiant_unique": data.get("id", "_"),
            "niveau_acces": get_fr_value(
                "inv_access_level",
                data.get("inv_access_level", "_"),
            ),
            "localisation_donnees": data.get("inv_data_location", "_"),
            "classification_securite": get_fr_value(
                "inv_security_classification",
                data.get("inv_security_classification", "_"),
            ),
            "langue": data.get("language", "_"),
            "identifiant_licence": data.get("license_id", "_"),
            "titre_licence": data.get("license_title", "_"),
            "url_licence": data.get("license_url", "_"),
            "responsable": data.get("maintainer", "_"),
            "email_responsable": data.get("maintainer_email", "_"),
            "date_creation_metadata": data.get("metadata_created", "_"),
            "date_modification_metadata": data.get("metadata_modified", "_"),
            "methodologie": data.get("methodologie", "_"),
            "nombre_ressources": data.get("num_resources", "_"),
            "nombre_tags": data.get("num_tags", "_"),
            "etat": data.get("state", "_"),
            "type": data.get("type", "_"),
            "frequence_mise_a_jour": get_fr_value(
                "update_frequency",
                data.get("update_frequency", "_"),
            ),
            "groupes": ", ".join(
                [group["display_name"] for group in data.get("groups", [])]
            ),
            "tags": ", ".join([tag["display_name"] for tag in data.get("tags", [])]),
            "formats_ressources": ", ".join(
                [resource["format"] for resource in data.get("resources", [])]
            ),
        }
    )

    return Document(page_content=content, metadata=metadata_f)


def get_ckan_package_list():
    url = f"https://www.donneesquebec.ca/recherche/api/action/package_list"
    response = requests.get(url, timeout=20)
    if "application/json" not in response.headers["Content-Type"]:
        raise Exception(f"Invalid content type {response.headers['Content-Type']}")
    package_list = response.json()
    return package_list.get("result", [])


def get_ckan_package_details(package_id: dict):
    response = requests.get(
        f"https://www.donneesquebec.ca/recherche/api/3/action/package_show?id={package_id}"
    )
    return response.json().get("result")


def get_fr_value(key: str, value: str):
    match key.lower():
        case "inv_access_level":
            match value:
                case "open":
                    return "ouvert"
                case _:
                    return value
        case "update_frequency":
            match value:
                case "quinquennial":
                    return "quinqennale"
                case "irregular":
                    return "irrégulière"
                case "bimonthly":
                    return "bimensuelle"
                case "annual":
                    return "annuelle"
                case "monthly":
                    return "mensuelle"
                case "weekly":
                    return "hebdomadaire"
                case "daily":
                    return "quotidienne"
                case "archives":
                    return "en archives"
                case "fortnightly":
                    return "tous les quinze jours"
                case "hourly":
                    return "horaire"
                case "quarterly":
                    return "trimestrielle"
                case "asNeeded":
                    return "au besoin"
                case "notPlanned":
                    return "non planifiée"
                case "semiannual":
                    return "semestrielle"
                case "triennial":
                    return "au trois ans"
                case "biennial":
                    return "au deux ans"
                case "continuous":
                    return "continue"
                case _:
                    return value
        case "inv_security_classification":
            match value:
                case "public":
                    return "publique"
                case _:
                    return value
        case _:
            return value
