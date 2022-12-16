from azqueuemanager.extension import ExtensionBaseClass, _parser_filter 
import json
from pathlib import Path
import logging

# Replease MYEXTENSION with the name of your extension

class JSONTransform(ExtensionBaseClass):
    """This class transforms json data into a format that can be used by the queue."""

    def __init__(
        self,
        json_in_file: str | None = None,
        json_out_file: str | None = 'out.json',
        json_data: str | None = None,
        parser_filter: _parser_filter=lambda x:x,
        parse_array: bool = False,
    ):

        # currently only the `parser filter` kwarg is required
        # for more information on the parser filter, see - github.com/kjaymiller/azqueuemanager/blob/main/azqueuemanager/extension.py
        super().__init__()

        # The REQUIRED_ARG is likely something that can determine how to ingest the data.
        # You can rename this to whatever you want and have as many as needed
        if not any([json_in_file, json_data]) or all([json_in_file, json_data]):
            logging.warning("Neither json_in_file or json_data were provided. You will not be able to use as an input_transformer.") 

        self.json_in_file = json_in_file
        self.json_out_file = json_out_file
        self.json_data = json_data
        self.pasrse_array = parse_array


    def transform_in(self):
        """This method transforms the data into a format that can be used by the queue."""
        if self.json_in_file:
            with open(self.json_in_file, 'rb') as file:
                data = json.load(file)

        if self.json_data:
            data = json.loads(self.json_data)

        for item in self.parser_filter(data):
            if self.pasrse_array:
                for sub_item in item:
                    yield sub_item
            else:
                yield item

    
    def transform_preview(self, data: str) -> dict[str, any]:
        """
        This method assists with creating previews.
        This is shouldn't send data to a response but provide a preview of the data that will be sent to the queue.
        """
        return json.loads(data.content)


    def transform_out(self, data: list[str]):
        """This method transform the data passed. If there is an output step it should be done here."""
        messages = [json.loads(msg.content) for msg in data]
        if self.json_out_file:
            with open(self.json_out_file, 'w', encoding="utf-8") as file:
                return json.dump(messages, file)

        return json.dumps(messages)
