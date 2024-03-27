from pyspark.sql import DataFrame
from pyspark.sql.functions import expr

class PhenotypingEngine:
    def __init__(self, data: DataFrame):
        self.data = data
        self.rules = []

    def add_rule(self, label: str, conditions: str):
        """
        Adds a new phenotyping rule.

        Parameters:
        - label (str): Label for the rule ('CASE' or 'CONTROL').
        - conditions (str): Conditions in SQL format.
        """
        # Validate input
        if label not in ['CASE', 'CONTROL']:
            raise ValueError("Label must be 'CASE' or 'CONTROL'")

        if not isinstance(conditions, str):
            raise TypeError("Conditions must be a string")

        # Append the rule
        self.rules.append({'label': label, 'conditions': conditions})

    def _apply_single_rule(self, rule: dict):
        """
        Applies a single phenotyping rule to the DataFrame.
    
        Parameters:
        rule (dict): The rule to apply, with the structure:
                     {'label': 'CASE' or 'CONTROL', 'conditions': 'SQL expression string'}
        """
        if rule['label'] == 'CASE':
            # Apply CASE rule and label non-matching rows as OTHER
            self.data = self.data.withColumn(
                'Phenotype',
                when(expr(rule['conditions']), 'CASE').otherwise('OTHER')
            )
        elif rule['label'] == 'CONTROL':
            # Apply CONTROL rule only to rows labeled as OTHER
            self.data = self.data.withColumn(
                'Phenotype',
                when(expr(rule['conditions']) & (col('Phenotype') == 'OTHER'), 'CONTROL').otherwise(col('Phenotype'))
            )

    def execute_phenotyping(self):
        # First, apply the CASE rule
        for rule in self.rules:
            if rule['label'] == 'CASE':
                self._apply_single_rule(rule)
    
        # Apply the CONTROL rule to the remaining rows
        for rule in self.rules:
            if rule['label'] == 'CONTROL':
                self._apply_single_rule(rule)
    
        # Assign 'OTHER' to rows not classified as CASE or CONTROL
        self.data = self.data.withColumn('Phenotype', when(col('Phenotype').isNull(), 'OTHER').otherwise(col('Phenotype')))


    def get_results(self):
        """
        Returns the DataFrame with applied phenotyping rules.

        Returns:
            DataFrame: The modified DataFrame with phenotyping labels.
        """
        # Assuming self.data is the DataFrame being modified
        return self.data