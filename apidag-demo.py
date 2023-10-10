from src import apidag
import asyncio

# Define the APIs as nodes
xkcd_node = apidag.APINode(
    id="xkcd_api",
    base_url="https://xkcd.com/{id}/info.0.json",
    input_params={"id": apidag.URLParam("id")},
    output_params={"title": "$.safe_title"},
    error_handlers={404: lambda input: {"title": ["No title found"]}},
)

dictionary_node = apidag.APINode(
    id="dictionary_api",
    base_url="https://api.dictionaryapi.dev/api/v2/entries/en/{word}",
    input_params={"word": apidag.URLParam("word")},
    output_params={"definition": "$..meanings[*].definitions[*].definition"},
    error_handlers={404: lambda input: {"definition": ["No definition found"]}}
)

# Define edges
def xkcd_to_dictionary(outputs):
    title = outputs.get("title")[0]
    words = title.split()
    first_word = words[0].lower() if words else ""
    return {"word": first_word}

edge = apidag.Edge(source="xkcd_api", target="dictionary_api", linkage_function=xkcd_to_dictionary)

# Define the flow
flow = apidag.APIFlow(nodes=[xkcd_node, dictionary_node], edges=[edge])

# Define callback
def callback(results):
    comic_data = results.get("xkcd_api", {})
    dictionary_data = results.get("dictionary_api", {})
    comic_title = comic_data['output'].get('title', ["Unknown Title"])[0]
    first_word = dictionary_data['input'].get('word', "Unknown Word")
    definitions = dictionary_data['output'].get('definition', [])
    if not isinstance(definitions, list):
        definitions = [definitions]

    if len(definitions) == 0:
        print(f"Comic #{comic_data['input']['id']} titled '{comic_title}' has its first word '{first_word}' undefined.")
    else:
        print(f"Comic #{comic_data['input']['id']} titled '{comic_title}' has its first word '{first_word}' defined as:")
        for definition in definitions:
            print("\t" + definition)

# Initialize the getter
getter = apidag.Getter(max_retries=3, workers=10)

# List of comic IDs to fetch in parallel
comic_ids = range(2630,2650)

# Define a main coroutine
async def main():
    tasks = [getter.run_flow(flow, {"id": str(comic_id)}, callback) for comic_id in comic_ids]
    await asyncio.gather(*tasks)

# Execute the main coroutine
asyncio.run(main())
