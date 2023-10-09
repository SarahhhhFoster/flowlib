import flowlib
import asyncio

# Define the APIs
xkcd_api = flowlib.API(
    base_url="https://xkcd.com/{id}/info.0.json",
    input_params={"id": flowlib.URLParam()},
    output_params={"title": "$.safe_title"},
    error_handlers={404: (lambda res: {"title": ["No title found"]})}
)

dictionary_api = flowlib.API(
    base_url="https://api.dictionaryapi.dev/api/v2/entries/en/{word}",
    input_params={"word": flowlib.URLParam()},
    output_params={"definition": "$..meanings[*].definitions[*].definition"},
    error_handlers={404: (lambda res: {"title": ["No definition found"]})}
)

def xkcd_to_dictionary(xkcd_response):
    title = xkcd_response.get("title", "")
    words = title.split()
    first_word = words[0].lower() if words else ""
    return [{"word": first_word}]

# Create a flow with an API linkage
flow = flowlib.APIFlow([flowlib.APILinkage(xkcd_api, dictionary_api, xkcd_to_dictionary)])

def callback(results):
    comic_data = results.get(xkcd_api.base_url, {})
    dictionary_data = results.get(dictionary_api.base_url, {})

    comic_title = comic_data['output'].get('title', ["Unknown Title"])[0]  # Make sure it's a list, taking the first element
    first_word = dictionary_data['input'].get('word', "Unknown Word")
    definitions = dictionary_data['output'].get('definition', [])
    if not isinstance(definitions, list):  # Ensure definitions is a list
        definitions = [definitions]

    if len(definitions) == 0:
        print(f"Comic #{comic_data['input']['id']} titled '{comic_title}' has its first word '{first_word}' undefined.")
    else:    
        print(f"Comic #{comic_data['input']['id']} titled '{comic_title}' has its first word '{first_word}' defined as:")
        for definition in definitions:
            print("\t" + definition)

# Initialize the getter
getter = flowlib.Getter(max_retries=3, workers=10)

# List of comic IDs to fetch in parallel
comic_ids = range(2630,2650)

# Define a main coroutine that will gather all tasks
async def main():
    tasks = [getter.run_flow(flow, {"id": comic_id}, callback) for comic_id in comic_ids]
    await asyncio.gather(*tasks)

# Run the main coroutine
asyncio.run(main())