
CLEAN_TEXT_PROMPT = '''
Your task is to clean and format a piece of dirty, webscraped text. Follow these strict guidelines:

1. Remove all HTML tags, CSS classes, and other web-related markup.
2. Eliminate extraneous whitespace, including multiple consecutive spaces, tabs, and newlines.
3. Correct obvious formatting issues such as missing spaces after punctuation or between words.
4. Preserve all original content, including misspellings, grammatical errors, and unconventional formatting choices made by the original author.
5. Maintain the original paragraph structure as closely as possible.
6. Do not add any new information, rewrite, rephrase, or alter the meaning of the text in any way.
7. Ensure that all text is left-aligned and consistently formatted.
8. Remove any repeated content that appears to be a result of scraping errors.
9. If there are clear section headings, ensure they are formatted consistently.
10. Preserve any lists, maintaining their original structure and numbering/bullet points.
11. Remove any scraping artifacts such as [advertisement] placeholders or navigation menu items that are not part of the main content.

Your output should be a clean, readable version of the original text, faithfully reproducing its content without any alterations to the substance or style of the writing. The goal is to improve readability without changing the original message or voice.

Please process the following webscraped text according to these guidelines:
'''

EXTRACT_ENTITIES_PROMPT = '''
Your task is to perform comprehensive Named Entity Recognition (NER) on the given text. Follow these guidelines:

1. Identify and extract all named entities, including but not limited to:
   - People
   - Organizations
   - Locations
   - Dates and times
   - Quantities
   - Products
   - Events
   - Websites
   - Nations or Geopolitical Entities
   - Laws and regulations

2. Simplify and standardize the extracted entities as much as possible:
   - Use full names for people (e.g., "John Fitzgerald Kennedy" instead of "Kennedy")
   - Use official names for organizations (e.g., "Microsoft Corporation" instead of "Microsoft")
   - Standardize date formats to ISO 8601 (YYYY-MM-DD)
   - Use full names for locations, including country for cities (e.g., "Paris, France")
   - Spell out abbreviations and acronyms where possible
   - Use official titles for laws and regulations
   - Standardize units of measurement to SI units where applicable

3. If an existing entity list is provided, align new entities with those already present:
   - Use the exact same naming convention if an entity already exists in the list
   - If a new entity is similar to an existing one, determine if they refer to the same thing and use the existing name if appropriate
   - If an entity in the existing list is mentioned in the text but in a different form, include it in your output using the standardized form from the existing list

4. Provide the output as a list, where each item contains the entity and its type. For example:
   John Fitzgerald Kennedy, Person
   Microsoft Corporation, Organization
   Paris, Location

   Ensure that:
   - Entities are listed in order of appearance in the text
   - If an entity could belong to multiple categories, include it multiple times with different types
   - Use the most specific type possible for each entity

   Do not number the entries in the list. We want to easily split by newline characters.
   Avoid commas in the entity names. We need to be able to easily split entities by commas for processing.

5. For ambiguous entities, choose the most likely category based on context, but aim for the highest possible granularity and specificity in categorization.

6. Include sub-entities where relevant. For example, if "United States Department of Defense" is mentioned, also include "United States" in the Locations category.

Process the following text and extract entities according to these guidelines. Be thorough and ensure no entities are missed:
'''

EXTRACT_RELATIONSHIPS_PROMPT = '''
Your task is to identify relationships between the provided entities based on the given text. Follow these guidelines:

1. Analyze the text and identify meaningful relationships between the entities.

2. Use the following concise but substantial relationship types:
   - IS_A: Denotes a type or classification relationship
   - PART_OF: Indicates a component or membership relationship
   - LOCATED_IN: Specifies a geographical or physical location relationship
   - WORKS_FOR: Denotes employment or affiliation
   - INTERACTS_WITH: Indicates a general interaction or association
   - CREATES: Denotes authorship, invention, or production
   - LEADS: Indicates leadership or management
   - OWNS: Denotes ownership or possession
   - OCCURS_ON: Specifies a temporal relationship
   - CREATED_ON: Specifies an entity coming to existence on this date
   - AFFECTS: Indicates influence or impact
   - RELATED_TO: Denotes a general relationship when others don't apply

3. Provide the output as a list, where each item contains three elements:
   entity1, relationship, entity2

   For example:
   John Fitzgerald Kennedy, LEADS, United States
   Microsoft Corporation, CREATES, Windows Operating System"
   Eiffel Tower, LOCATED_IN, Paris

   Do not number the entries in the list. We want to easily split by newline characters.
   Avoid commas in the entity or relationship names. We need to be able to easily split entities by commas for processing.

4. Ensure that relationships are directional and meaningful.

5. If an entity is involved in multiple relationships, include all relevant relationships.

6. Base the relationships solely on the information provided in the text, not on external knowledge.

Process the following text and extract relationships between the provided entities. Be as thorough and complete as possible. Ensure no relationships are missed.
'''