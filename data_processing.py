import pandas as pd


class DataProcessing(object):
    """
    Class for data processing in the provided csv file
    """
    df = pd.read_csv("PATH TO CSV FILE")
    # Timestamp conversion to datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'].astype(int), unit='s').dt.normalize()

    def filter_info(self):
        """
        Returns information about possible filtering
        """
        datatypes = dict(self.df.dtypes)
        attrs = []
        attrs_values = []
        for key, value in datatypes.items():
            attrs.append(key)
            attrs_values.append([str(key), str(value)])
        response = {"attributes": attrs,
                    "attributes-values": attrs_values
                    }
        return response

    def timeline_data(self, params):
        """
        Returns data with timeline information according to input parameters
        """
        # Required parameters
        start_date = self.check_parameter('startDate', params)
        end_date = self.check_parameter('endDate', params)
        sum_type = self.check_parameter('Type', params)
        grouping = self.check_parameter('Grouping', params)
        if not start_date or not end_date or not sum_type or not grouping:
            return "Required parameters startDate/endDate/Type/Grouping are not provided"

        # Optional parameters
        asin = self.check_parameter('asin', params)
        brand = self.check_parameter('brand', params)
        source = self.check_parameter('source', params)
        stars = self.check_parameter('stars', params)

        # Filter data by provided time frame
        data = self.df.loc[(self.df['timestamp'] > start_date) & (self.df['timestamp'] < end_date)]

        # Filter data by provided optional parameters
        if asin:
            data = data.loc[(self.df['asin'] == asin)]
        if brand:
            data = data.loc[(self.df['brand'] == brand)]
        if source:
            data = data.loc[(self.df['source'] == source)]
        if stars:
            data = data.loc[(self.df['stars'].isin([stars]))]

        # Aggregate data weekly/bi-weekly/monthly
        if grouping == "weekly":
            data = data.groupby(pd.Grouper(key='timestamp', freq='W'))
        elif grouping == "bi-weekly":
            data = data.groupby(pd.Grouper(key='timestamp', freq='2W'))
        elif grouping == "monthly":
            data = data.groupby(pd.Grouper(key='timestamp', freq='M'))

        # Aggregate final data
        if sum_type == "cumulative":
            data = data.size().cumsum()
        elif sum_type == "usual":
            data = data.size()

        result = []
        for key, value in data.items():
            a = {"date": str(key).split()[0], "value": int(value)}
            result.append(a)

        return result

    @staticmethod
    def check_parameter(key, params):
        """
        Checks and assigns parameters
        """
        try:
            res = params.get(key)
        except KeyError:
            pass
        return res
