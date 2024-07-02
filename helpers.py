import xml.etree.ElementTree as ET
import urllib.request
import requests
import os


class OfficielePublicatie:
    def __init__(self):
        self.identifier = []
        self.title = []
        self.type = []
        self.language = []
        self.authority = []
        self.creator = []
        self.modified = []
        self.temporal = []
        self.spatial = []
        self.alternative = []
        self.date = []
        self.hasVersion = []
        self.source = []
        self.requires = []
        self.isPartOf = []
        self.isrequiredby = []
        self.isreplacedby = []
        self.hasPart = []
        self.subject = []
        self.available = []
        self.abstract = []
        self.publisher = []
        self.issued = []
        self.replaces = []
        self.aanhangsel = []
        self.aanhangselnummer = []
        self.adviesRvs = []
        self.bedrijfsnaam = []
        self.behandeldDossier = []
        self.betreft = []
        self.betreftopschrift = []
        self.betreftRegeling = []
        self.bijlage = []
        self.datumBrief = []
        self.datumIndiening = []
        self.datumOndertekening = []
        self.datumOntvangst = []
        self.datumTotstandkoming = []
        self.datumVergadering = []
        self.deurwaardersdossier = []
        self.documentstatus = []
        self.documenttitel = []
        self.dossiernummer = []
        self.dossiertitel = []
        self.effectgebied = []
        self.einddatum = []
        self.datumEindeReactietermijn = []
        self.eindpagina = []
        self.externeBijlage = []
        self.gebiedsmarkering = []
        self.gemeentenaam = []
        self.geometrie = []
        self.geometrie = []
        self.geometrielabel = []
        self.hectometernummer = []
        self.heeftMededeling = []
        self.hoofddocument = []
        self.huisnummer = []
        self.huisletter = []
        self.huisnummertoevoeging = []
        self.indiener = []
        self.handelingenitemnummer = []
        self.jaargang = []
        self.kadastraleSectie = []
        self.ketenid = []
        self.ligtInGemeente = []
        self.ligtInProvincie = []
        self.materieelUitgewerkt = []
        self.mededelingOver = []
        self.ondernummer = []
        self.ontvanger = []
        self.organisatietype = []
        self.perceelnummer = []
        self.persoonsgegevens = []
        self.plaatsTotstandkoming = []
        self.postcode = []
        self.postcodeHuisnummer = []
        self.provincie = []
        self.provincienaam = []
        self.publicatienummer = []
        self.publicatienaam = []
        self.besluitReferendabiliteit = []
        self.referentienummer = []
        self.rijkswetnummer = []
        self.bekendmakingBetreffendePlan = []
        self.startdatum = []
        self.startpagina = []
        self.straatnaam = []
        self.subrubriek = []
        self.sysyear = []
        self.sysnumber = []
        self.sysseqnumber = []
        self.terinzageleggingBG = []
        self.terinzageleggingOP = []
        self.typeVerkeersbesluit = []
        self.verdragnummer = []
        self.vereisteVanBesluit = []
        self.vergaderjaar = []
        self.verkeersbordcode = []
        self.versieinformatie = []
        self.versienummer = []
        self.vraagnummer = []
        self.waterschapsnaam = []
        self.wegcategorie = []
        self.weggebruiker = []
        self.wegnummer = []
        self.woonplaats = []
        self.zittingsdatum = []
        self.product_area = []
        self.content_area = []
        self.datumTijdstipWijzigingWork = []
        self.datumTijdstipWijzigingExpression = []
        self.url = []
        self.prefferedUrl = []
        self.itemUrl = []
        self.timestamp = []

    @classmethod
    def from_xml_element(
        cls, element: ET.Element, namespaces: dict
    ) -> "OfficielePublicatie":
        op = cls()
        for child in element.findall(".//*", namespaces):
            tag = cls.parse_namespaced_tag(child.tag)

            if tag not in op.__dict__:
                continue

            if child.attrib:
                key, value = list(child.attrib.items())[0]
                value_dict = {value: child.text.strip() if child.text else ""}
                # value_dict['value'] = child.text.strip() if child.text else ""
                op.__dict__[tag].append(value_dict)
            else:
                op.__dict__[tag].append(child.text.strip() if child.text else "")

        return op

    @staticmethod
    def parse_namespaced_tag(tag: str) -> str:
        return tag.split("}", 1)[-1] if "}" in tag else tag

    def __iter__(self):
        for key in self.__dict__:
            yield {key: getattr(self, key)}

    def __repr__(self):
        return f"<OfficielePublicatie identifier={self.identifier}, title={self.title}>"


def ontsluit_handelingen(query_part2, startRecord=1, maximumRecords=10):
    # Connectiestring met basis parameters, waarbij al gefilterd wordt op 'officielepublicaties'
    url = "https://repository.overheid.nl/sru"
    params = {
        "operation": "searchRetrieve",
        "version": "2.0",
        "query": "c.product-area==officielepublicaties",
        "startRecord": startRecord,
        "maximumRecords": maximumRecords,
        "recordSchema": "gzd",
    }

    # Als er een tweede deel voor de query is, voeg deze toe aan de originele query en vervang de query in de parameters
    if query_part2:
        params["query"] += query_part2

    # Voer de request uit
    response = requests.get(url, params=params)

    # Controleer of de request gelukt is
    if response.status_code == 200:
        # Zet de response om naar een XML object (bytes)
        xml_data = response.content

        # Parse de XML vanuit bytes naar een ElementTree object
        root = ET.fromstring(xml_data)

        # Return het ElementTree object
        return root
    else:
        print(f"Request failed with status code: {response.status_code}")


def retrieve_publications(query_list=[], max_records=10, start_record=1):
    query_concatter = " AND "
    query_part2 = ""
    for value in query_list:
        query_part2 += f"{query_concatter}{value}"

    root = ontsluit_handelingen(query_part2, start_record, max_records)

    namespaces = {
        "sru": "http://docs.oasis-open.org/ns/search-ws/sruResponse",
        "gzd": "http://standaarden.overheid.nl/sru",
    }

    ops = []
    # Loop door iedere record in de records container
    for record in root.findall(".//sru:records/sru:record", namespaces):
        # Genest in <sru:recordData>
        record_data = record.find(".//sru:recordData", namespaces)
        if record_data is not None:
            ob = OfficielePublicatie.from_xml_element(record_data, namespaces)
            ops.append(ob)

    return ops


def strip_xml_to_text(xml_url):
    response = urllib.request.urlopen(xml_url)
    tree = ET.parse(response)

    # Parse the XML file
    root = tree.getroot()  # Get the root element from the ElementTree

    # Extract text and replace newline characters with a space
    extracted_text = (
        ET.tostring(root, encoding="utf8", method="text")
        .decode("utf-8")
        .replace("\n", " ")
    )

    return extracted_text


def split_string(input_string):
    if ";" in input_string:
        parts = input_string.split(";", 1)  # Split only at the first occurrence of ';'
        return parts
    else:
        return [input_string]


def validate_references(ref_list: list):
    corrected_ref_tuples = []

    if len(ref_list) == 0:
        return corrected_ref_tuples
    else:
        for i in ref_list:
            dossier = False
            result_split_string = split_string(i)
            query = []
            if len(result_split_string) == 1:
                query.append("w.dossiernummer ==" + result_split_string[0])
                query.append('dt.type == "Kamerstuk"')
                dossier = True
            elif len(result_split_string) == 2:
                query.append("w.dossiernummer ==" + result_split_string[0])
                query.append("w.ondernummer ==" + result_split_string[1])
            pubs = retrieve_publications(query)
            if len(pubs) == 0:
                continue
            else:
                if dossier == True:
                    corrected_ref_tuples.append((i, pubs))
                elif dossier == False:
                    pub = pubs[0]
                    corrected_ref_tuples.append((i, pub))
        return corrected_ref_tuples


def list_file_paths(directory: str):
    # List to hold file paths
    file_paths = []

    # Walking through the directory
    for root, dirs, files in os.walk(directory):
        for file in files:
            # Constructing the file path
            path = os.path.join(root, file)
            # Adding the path to the list
            file_paths.append(path)

    return file_paths
