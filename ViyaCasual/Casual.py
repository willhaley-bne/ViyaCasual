from ViyaCasual.CAS.CAS import CAS
from copy import copy
from uuid import uuid4


class Casual(CAS):

    def get_table(self, name=None, library=None):
        new_table = copy(self.table_base)
        new_table.source_cas = name
        new_table.source_caslib = library
        new_table.get_source_data()
        return new_table

    def update_table(self, **kwargs):
        new_table = copy(self.table_base)
        new_table.cas_table_name = self.__validate_args('name', kwargs)

        if 'library' in kwargs:
            new_table.caslib = kwargs['library']

        if 'data' in kwargs and isinstance(kwargs['data'], 'DataFrame'):
            new_table.source_data = kwargs['data']
            new_table.update_from_records(kwargs['data'])
        elif 'sql' in kwargs:
            new_table.source_sql = kwargs['sql']
            new_table.update_from_source()
        else:
            raise Exception('Must be passed SQL or Data so it won\'t be empty')
        return new_table

    def create_temp_table(self, **kwargs):
        table = self.create_new_table(name=uuid4())
        table.clean_up = True
        return table

    @staticmethod
    def __validate_args(item, passed):
        if item not in passed:
            raise Exception('%s is required' % str(item))
        return passed[item]

    def get_decision(self, **kwargs):
        decision = copy(self.decision_base)
        decision.rule_name = self.__validate_args('name', kwargs)
        decision.model_lib = self.default_decision_lib
        decision.model_table_name = self.default_decision_table

        if 'library' in kwargs:
            decision.model_lib = kwargs['library']
        if 'table_name' in kwargs:
            decision.model_table_name = kwargs['table_name']
        decision.set_details()

        return decision

    def run_decision(self, **kwargs):

        if 'name' not in kwargs.keys() and 'decision' not in kwargs.keys():
            raise Exception('Please provide a decision name or a decision object')

        if 'input_data' not in kwargs.keys():
            raise Exception('Please provide the input_data as a Pandas Dataframe')

        # todo; Add test to make sure args passed are correct types
        '''
        if isinstance(kwargs['input_data'], 'pandas.DataFrame') is False:
            raise Exception('Please provide the input_data as a Pandas Dataframe')
        '''
        if 'name' in kwargs.keys():
            dec = self.get_decision(
                name=kwargs['name'],
                library=kwargs['library'],
                table_name=kwargs['table_name']
            )
            dec.set_input(kwargs['input_data'])
            dec.exec()
            return dec.get_results()

        if 'decision' in kwargs.keys():
            kwargs['decision'].exec()
            kwargs['decision'].set_input(kwargs['input_data'])
            return kwargs['decision'].get_results()
