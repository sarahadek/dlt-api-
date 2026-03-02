import dlt
import dlt
from itertools import islice
from dlt.sources.rest_api import rest_api_source

def openlibrary_source(query: str = "harry potter"):

    return rest_api_source({
        "client": {
            "base_url": "https://openlibrary.org",
        },
        "resource_defaults": {
            "primary_key": "key",
            "write_disposition": "replace",
        },
        "resources": [
            {
                "name": "books",
                "endpoint": {
                    "path": "search.json",
                    "params": {
                        "q": query,
                        "limit": 100,
                    },
                    "data_selector": "docs",
                    "paginator": {
                        "type": "offset",
                        "limit": 100,
                        "offset_param": "offset",
                        "limit_param": "limit",
                        "total_path": "numFound",
                    },
                },
            },
        ],
    })


pipeline = dlt.pipeline(
    pipeline_name="open_library_pipeline",
    destination="duckdb",
    dataset_name="open_library_data",
    progress="log" # logs the pipeline run (Optiona)
)

if __name__ == "__main__":
    # 1. Initialize the source
    source = openlibrary_source()
    
    # 2. Run the pipeline and SAVE the result to load_info
    # This is the "Action" step where data moves from API -> DuckDB
    load_info = pipeline.run(source)
    
    # 3. Print the result so you can verify the load worked
    print(load_info)