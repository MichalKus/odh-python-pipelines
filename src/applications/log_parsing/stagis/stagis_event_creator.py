from common.log_parsing.list_event_creator.event_creator import EventCreator


class StagisEventCreator(EventCreator):
    """
    Replaces extracted task value with the one from dictionary
    """

    def _convert_row_to_event_values(self, row):
        parse_result = self._parser.parse(row["message"])
        if parse_result:
            parse_result[0] = self.__replace_task_name(parse_result[0])
        return parse_result

    @staticmethod
    def __replace_task_name(text):
        dictionary = {
            "TVA Delta Server respond": "TVA Delta Server response",
            "TVA Delta Request Starting": "TVA Delta Server request",
            "Received Delta Server Notification": "Notification",
            "Model state after committing transaction": "Committing Transaction"
        }
        if text in dictionary:
            text = text.replace(text, dictionary[text])
        return text
