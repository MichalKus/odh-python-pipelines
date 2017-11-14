from common.log_parsing.list_event_creator.event_creator import EventCreator


class MultilineEventCreator(EventCreator):
    def __init__(self, metadata, parser, fold_functions, line_delimiter="\n"):
        if metadata.get_fields_amount() != len(fold_functions):
            raise AssertionError("count of fold functions should be equal to fields amount")
        EventCreator.__init__(self, metadata, parser)
        self.parser = parser
        self.fold_functions = fold_functions
        self.line_delimiter = line_delimiter

    def _convert_row_to_event_values(self, row):
        lines = row["message"].split(self.line_delimiter)
        parsed_lines = map(lambda item: self.parser.parse(item), lines)
        zipped_lines = [list(i) for i in zip(*parsed_lines)]
        return [reduce(self.fold_functions[i], line) for i, line in enumerate(zipped_lines)]
