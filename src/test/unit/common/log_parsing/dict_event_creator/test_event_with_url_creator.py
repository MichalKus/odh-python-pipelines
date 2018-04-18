import unittest

from common.log_parsing.dict_event_creator.event_with_url_creator import EventWithUrlCreator


class EventCreatorTestCase(unittest.TestCase):

    row = {"url": "/vod-service/v2/gridscreen/omw_hzn4_vod/crid:~~2F~~2Fschange.com?country=nl&language=nl&profileId=9727801_nl#23MasterProfile"}

    def test_event_create_correct_fields_and_values(self):
        event_creator = EventWithUrlCreator(url_field="url")
        self.assertEquals(
            {
                "country": "nl",
                "language": "nl",
                "profile_id": "9727801_nl#23MasterProfile",
                "action": "/vod-service/v2/gridscreen/omw_hzn4_vod/crid:~~2F~~2Fschange.com"
            },
            event_creator.create(self.row)
        )



if __name__ == '__main__':
    unittest.main()
