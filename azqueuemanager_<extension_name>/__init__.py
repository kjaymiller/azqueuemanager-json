from azqueuemanager.extension import ExtensionBaseClass, _parser_filter 
import json
from pathlib import Path

# Replease MYEXTENSION with the name of your extension

class MYEXTENSION(ExtensionBaseClass):
    """This class transforms json data into a format that can be used by the queue."""

    def __init__(
        self,
        REQUIRED_ARG: any,
        OPTIONAL_ARG: any = None,
        parser_filter: _parser_filter=lambda x:x,
    ):

        # currently only the `parser filter` kwarg is required
        # for more information on the parser filter, see - github.com/kjaymiller/azqueuemanager/blob/main/azqueuemanager/extension.py
        super().__init__()

        # The REQUIRED_ARG is likely something that can determine how to ingest the data.
        # You can rename this to whatever you want and have as many as needed
        self.REQUIRED_ARG = REQUIRED_ARG

        # The OPTIONAL_ARG is likely something that assists REQUEIRED ARGS to ingest the data.
        #  You can rename this to whatever you want and have as many as needed
        self.OPTIONAL_ARG= optional_arg

    def transform_in(self):
        """This method transforms the data into a format that can be used by the queue."""
        for item in self.parser_filter(REQUIRED_ARG):
            yield item

    
    def transform_in_place(self, data: str):
        """
        This method assists with creating previews.
        This is shouldn't send data to a response but provide a preview of the data that will be sent to the queue.
        """
        return data['content']


    def transform_out(self, data: list[str]):
        """This method transform the data passed. If there is an output step it should be done here."""
        for msg in data:
            yield self.transform_in_place(msg)