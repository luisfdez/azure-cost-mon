import requests
import datetime
from pandas import DataFrame
from prometheus_client.core import CounterMetricFamily

base_columns = ['billingPeriodId', 'currencyCode']
cost_column = ['totalUsage']


def convert_json_df(data):
    """
    Convert the API response (JSON document) to a data frame
    """
    columns = base_columns + cost_column
    content = list()
    for item in data:
        line = list()
        for c in columns:
            value = item[c]
            try:
                value = value.lower()
            except:
                pass

            line.append(value)

        content.append(line)

    df = DataFrame(data=content, columns=columns)

    return df


class AzureEABalanceCollector(object):
    """
    Class to export Azure billing and usage information extracted via ea.azure.com 
    in Prometheus compatible format.
    """

    def __init__(self, metric_name, enrollment, token, timeout):
        """
        Constructor.
        
        :param metric_name: Name of the timeseries
        :param enrollment: ID of the enterprise agreement (EA)
        :param token: Access Key generated via the EA portal
        :param timeout: Timeout to use for the request against the EA portal
        """
        self._metric_name = metric_name
        self._enrollment = enrollment
        self._token = token
        self._timeout = timeout

    def _get_azure_data(self):
        """
        Request the billing data from the Azure API and return a dict with the data

        :return: JSON document of usage and billing information for the given month
        """


        headers = {"Authorization": "Bearer {0}".format(self._token)}
        url = "https://consumption.azure.com/v3/enrollments/{0}/balancesummary".format(self._enrollment)

        print("About to query Azure data from {0}".format(url))
        rsp = requests.get(url, headers=headers, timeout=self._timeout)
        rsp.raise_for_status()

        print("Azure data fetched: {0}".format(rsp.json()))

        return rsp.json()

    def _create_counter(self):
        """
        Create a counter instance.
        
        :return: prometheus_client counter instance
        """
        description = "Costs billed to Azure Enterprise Agreement {}".format(self._enrollment)
        c = CounterMetricFamily(self._metric_name, description, labels=base_columns)
        return c

    def describe(self):
        """
        Default registry calls "collect" if "describe" is not existent to determine timeseries names. 
        Don't want that because it issues a request to the billing API.
            
        :return: The metrics we are collecting.
        """
        return [self._create_counter()]

    def collect(self):
        """
        Yield the metrics.
        """
        c = self._create_counter()

        usage_data = self._get_azure_data()
        df = convert_json_df(usage_data)
        groups = df.groupby(base_columns).sum()

        for labels, value in groups.iterrows():
            c.add_metric(labels, value.totalUsage)

        yield c
