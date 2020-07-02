from ViyaCasual.Viya.ViyaConnection import ViyaConnection
import unittest
import warnings


class TestConnectionFail(ViyaConnection):
    url = 'test.com'
    user_name = 'tester'
    password = 'password'
    auto_connect = False


class TestConnection(ViyaConnection):
    url = 'reports.boddienoell.com'
    user_name = 'willhaley'
    password = 'wi1587ha'


class TestBasics(unittest.TestCase):

    def setUp(self):
        warnings.filterwarnings(action="ignore", message="unclosed",
                                category=ResourceWarning)
        self.viya = TestConnection()

    def test_configs_port(self):
        temp = TestConnectionFail()
        self.assertEqual(5570, temp.port)

    def test_configs_user_name(self):
        temp = TestConnectionFail()
        self.assertEqual('tester', temp.user_name)

    def test_configs_password(self):
        temp = TestConnectionFail()
        self.assertEqual('password', temp.password)

    def test_configs_url(self):
        temp = TestConnectionFail()
        self.assertEqual('test.com', temp.url)

    def test_swat_connection(self):
        server_details = self.viya.get_server_status()
        self.assertEqual(server_details['About']['CAS'], 'Cloud Analytic Services')

    def test_ctl_connection(self):
        self.assertEqual(str(type(self.viya.ctl)), '<class \'sasctl.core.Session\'>')

    def test_increase_row_download_limit(self):
        new_limit = 11000
        self.viya.increase_row_download_limit(new_limit)
        self.assertEqual(self.viya.swat.get_option('cas.dataset.max_rows_fetched'), new_limit)

    def test_get_cas_table(self):
        data = self.viya.get_cas_table('SYSTEM', 'SystemData')
        server_details = self.viya.get_server_status()
        self.assertEqual(server_details['About']['System']['Hostname'], data['machine'].values[0])


if __name__ == '__main__':
    unittest.main()
