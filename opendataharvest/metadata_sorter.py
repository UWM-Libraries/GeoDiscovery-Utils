import json
import yaml

class MetadataSorter:
    def __init__(self, config_file='format_keywords.yaml'):
        self.load_config(config_file)

    def load_config(self, config_file):
        with open(config_file, 'r') as file:
            self.config = yaml.safe_load(file)
        self.keywords = self.config.get('keywords', {})
        self.resource_types = self.config.get('resource_types', {})
        self.formats = self.config.get('formats', {})

    def classify_record(self, record):
        resource_class = self.find_match(record, self.keywords, "Other")
        resource_type = self.find_match(record, self.resource_types, "Other")
        format_type = self.find_match(record, self.formats, "Other")
        return {
            "resource_class": resource_class,
            "resource_type": resource_type,
            "format": format_type
        }

    def find_match(self, record, category_dict, default_value):
        for category, keywords in category_dict.items():
            for keyword in keywords:
                if self.record_contains_keyword(record, keyword):
                    return category
        return default_value

    def record_contains_keyword(self, record, keyword):
        for key, value in record.items():
            if isinstance(value, str) and keyword.lower() in value.lower():
                return True
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, str) and keyword.lower() in item.lower():
                        return True
        return False

    def format_fetcher(self, record):
        classifications = self.classify_record(record)
        return classifications

if __name__ == "__main__":
    # Example usage:
    with open('dcat_record.json', 'r') as file:
        record = json.load(file)
    
    sorter = MetadataSorter()
    classifications = sorter.format_fetcher(record)

    print(f'Classifications: {classifications}')
