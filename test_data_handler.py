from datetime import datetime, timedelta
import unittest
import pandas as pd
from data_handler import DataHandler


class TestDataHandler(unittest.TestCase):

    def setUp(self):
        self.dh = DataHandler("db.db")

    def test_download_data(self):
        data = self.dh.download_data()
        numberOfRows = len(data)

        self.assertEqual(numberOfRows, 24999)
    
    def test_process_data(self):         

        def check_member_subscriptions(df):
            dates_start = df['date_start'].values.tolist()
            dates_end   = df['date_end'].values.tolist()
            types = df['type'].values.tolist()
            
            date_type_sequence = list(zip(dates_end, dates_start[1:], types, types[1:]))
            
            for date_end, next_date_start, type, next_type in date_type_sequence:

                date_end = datetime.strptime(date_end,"%Y-%m-%d")
                next_date_start = datetime.strptime(next_date_start,"%Y-%m-%d")

                if next_date_start - date_end <= timedelta(days=3):
                    if type == next_type:
                        return False
            return True

        data = self.dh.download_data()
        processedData = self.dh.process_data(data)
        df=pd.DataFrame(processedData,columns=("member_id","date_start","date_end","type"))
        checked_data = df.groupby('member_id').apply(check_member_subscriptions)

        self.assertEqual(checked_data.size, checked_data.sum())

suite = unittest.TestLoader().loadTestsFromTestCase(TestDataHandler)
unittest.TextTestRunner(verbosity=2).run(suite)