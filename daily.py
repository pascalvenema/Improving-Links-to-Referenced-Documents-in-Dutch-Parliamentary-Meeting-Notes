import re
from helpers import retrieve_publications, strip_xml_to_text, validate_references


def detect_and_reconcile_explicit_references(string_stage0: str):
    pattern = re.compile(
        r"""
    ((\d{5}(?:-[0-9A-Z]{1,4})?)\s*,?\s*nr\.?\s*(\d{1,4}))|
    ((?<!Z)(\d{5}(?:-[0-9A-Z]{1,4})?)(?:-\d{1,4})?)|
    (nr\.?\s*(\d{1,4})\s*\((\d{5}(?:-[0-9A-Z]{1,4})?)\))
    """,
        re.VERBOSE,
    )

    # Initialise a list to hold all references found
    references = []

    # Find all matches of the pattern in the text
    matches = pattern.finditer(string_stage0)

    for match in matches:
        if match.group(2) and match.group(3):
            # Format: "file, nr. document"
            ref = f"{match.group(2)};{match.group(3)}"

        elif match.group(4):
            # Only replace the last hyphen with a semicolon
            ref = re.sub(r"-(?=[^-]*$)", ";", match.group(4))

        elif match.group(7) and match.group(8):
            # Format: "nr. document (file)"
            ref = f"{match.group(8)};{match.group(7)}"

        else:
            # If the match doesn't fit the expected patterns, continue without adding
            continue
        references.append(ref)

    # Remove all matches from the text
    string_stage1 = pattern.sub("", string_stage0)

    # Return the references as a list and the modified text
    return references, string_stage1


def detect_and_reconcile_local_aliases(string_stage1, already_existing_references_set):
    # Remove document numbers from the identifiers so that only the dossier numbers remain
    modified_set = {item.split(";")[0] for item in already_existing_references_set}
    # Only run this logic if one dossier number is found to prevent false positives
    if len(modified_set) == 0 or len(modified_set) > 1:
        return []
    else:
        document_numbers = []
        # Get the dossier number
        dossier_number = next(iter(modified_set))
        # Detection
        pattern = r"(?i)(?<!griffie\s)(?<!,\s)(?<!,)(?:stuk\s+nr|stuk\snrs|nr)\.s?\s+(\d+)(?!\(\d{5}\))"
        document_numbers = re.findall(pattern, string_stage1)
        # "Reconciliation", i.e. adding the dossier number to the document numbers
        document_numbers = [f"{dossier_number};{num}" for num in document_numbers]

        return document_numbers


def extract_links_from_minute(minute_id=None, xml_url=None, existing_references=None):
    """
    Extracts links (already existing + automatically detected) from a minute based on the given minute_id.

    Args:
        xml_url (str): The URL to the XML of the input minute (Handeling).

    Returns:
        tuple: A tuple containing the following:
            - already_existing_references (list): A list of existing references from the minute.
            - validated_extracted_explicit_references (list): A list of tuples of validated explicit references extracted from the minute, in the form of (reference_id, resource).
            - validated_extracted_local_alias_references (list): A list of tuples of validated local alias references extracted from the minute, in the form of (reference_id, resource).
    Raises:
        ValueError: If no publications are found for the given minute_id.
    """
    xml_url = xml_url
    existing_references = existing_references
    if xml_url == None and existing_references == None:
        publications = retrieve_publications(
            query_list=["dt.identifier == " + minute_id]
        )
        if len(publications) == 0:
            raise ValueError("No publications found for the given minute_id.")
        else:
            minute = publications[0]
            xml_url = next((d["xml"] for d in minute.itemUrl if "xml" in d), None)
            existing_references = sorted(minute.behandeldDossier)

    string_stage0 = strip_xml_to_text(xml_url)

    # Get all existing references from the minute
    existing_references = sorted(existing_references)
    validated_existing_references = validate_references(existing_references)

    # Extract explicit references from the text
    extracted_explicit_references, string_stage1 = (
        detect_and_reconcile_explicit_references(string_stage0)
    )
    # Validate the extracted explicit references, and return a tuple for each validated reference in the form of (reference_id, resource)
    validated_extracted_explicit_references = validate_references(
        extracted_explicit_references
    )

    # Extract implicit local alias references from the text
    extracted_local_alias_references = detect_and_reconcile_local_aliases(
        string_stage1, set(existing_references)
    )
    # Validate the extracted local alias references, and return a tuple for each validated reference in the form of (reference_id, resource)
    validated_extracted_local_alias_references = validate_references(
        extracted_local_alias_references
    )

    return (
        validated_existing_references,
        validated_extracted_explicit_references,
        validated_extracted_local_alias_references,
    )



# Sample usage

# tuple_existing_refs, tuple_explicit_refs, tuple_local_alias_refs = extract_links_from_minute(
#     xml_url="https://zoek.officielebekendmakingen.nl/h-tk-20102011-54-6.xml",
#     already_existing_references=[
#         "32563;12",
#         "32563;13",
#         "32563;14",
#         "32563;15",
#         "32563;16",
#         "32563;17",
#         "32563;18",
#         "32563;19",
#         "32563;20",
#         "32563;21",
#         "32563;22",
#     ],
# )

# tuple_existing_refs, tuple_explicit_refs, tuple_local_alias_refs = extract_links_from_minute(
#     minute_id="h-tk-20102011-54-6"
# )

# print("Original references: ")
# print([tuple[0] for tuple in tuple_existing_refs])
# print("Validated explicit references: ")
# print([tuple[0] for tuple in tuple_explicit_refs])
# print("Validated local alias references: ")
# print([tuple[0] for tuple in tuple_local_alias_refs])
