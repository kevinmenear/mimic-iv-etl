from pyspark.sql import DataFrame
from pyspark.sql.functions import expr

class PhenotypingEngine:
    def __init__(self, data: DataFrame):
        self.data = data
        self.rules = []

    def add_rule(self, rule: dict):
        """
        Adds a new phenotyping rule.

        Parameters:
        rule (dict): A dictionary defining the rule. Expected to have 'conditions' and 'label'.

        'conditions' should be a string of all conditions (e.g. "'gender' = 'male' AND 'anchor_age' > 65")
        'outcome' should be the phenotype label (CASE, CONTROL, OTHER).
        """
        if not rule.get('conditions'):
            raise ValueError("'conditions' must be provided.")
        if not isinstance(rule['conditions'], str):
            raise TypeError("Conditions must be provided as a single string.")
            
        if not rule.get('label'):
            raise ValueError("'label' must be provided.")
        if rule.get('label').upper() not in ['CASE', 'CONTROL']:
            raise ValueError("Label must be either 'CASE' or 'CONTROL' (case-insensitive)")

        self.rules.append(rule)

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