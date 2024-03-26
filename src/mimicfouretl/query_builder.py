class QueryBuilder:
    def __init__(self, dataset, columns=None, filters=None):
        """
        Initializes the QueryBuilder object.

        Parameters:
        - dataset (str): The name of the dataset from which the query will be built.

        Attributes:
        - selected_columns (list): Columns selected for the query.
        - filters (list): Conditions applied to filter the query.
        - joins (list): Join conditions for combining with other datasets.
        """
        self.dataset = dataset
        if columns:
            if isinstance(columns, list):
                self.columns = columns
            else:
                self.columns = [columns]
        else:
            self.columns = []
        if filters:
            if isinstance(filters, list):
                self.filters = filters
            else:
                self.filters = [filters]
        else:
            self.filters = []
        self.joins = []

    def select_columns(self, columns):
        """
        Selects specific columns for the query.

        Parameters:
        - columns (list of str): A list of column names to be included in the query.
        """
        if isinstance(columns, list):
            self.columns.extend(columns)
        else:
            self.columns.append(columns)

    def apply_filters(self, condition):
        """
        Applies filter conditions to the query.

        Parameters:
        - condition (str or tuple): The filter condition(s) to apply. Can be a single condition as a string or multiple conditions in a tuple.
        """
        if isinstance(filters, list):
            self.filters.extend(condition)
        else:
            self.filters.append(condition)

    def join_with(self, other_qb, join_type, on_conditions):
        """
        Joins the current dataset with another dataset.

        Parameters:
        - other_qb (QueryBuilder): Another QueryBuilder object representing the dataset to join.
        - join_type (str): The type of join (e.g., 'inner', 'left').
        - on_conditions (str or list): The column name(s) to join on, either as a single string or a list for multiple join keys.
        """
        if not isinstance(on_conditions, list):
            on_conditions = [on_conditions] 
        joined_on = ' AND '.join(on_conditions)
        join_query = f"{join_type.upper()} JOIN `{other_qb.dataset}` ON {joined_on}"
        self.joins.append(join_query)
        self.columns.extend(other_qb.columns)
        self.filters.extend(other_qb.filters)

    def generate_query(self):
        """
        Constructs and returns the final SQL query string.

        Returns:
        - str: A string representing the complete SQL query, combining selected columns, filters, and joins.
        """
        select_clause = f"SELECT {', '.join(self.columns)}" if self.columns else "SELECT *"
        from_clause = f"FROM `{self.dataset}`"
        if len(self.joins) > 0:
            join_clauses = "\n".join(self.joins)
            join_clauses += "\n"
        else:
            join_clauses = ''
        where_clause = f"WHERE {' AND '.join(self.filters)}" if self.filters else ""

        return f"{select_clause}\n{from_clause}\n{join_clauses}{where_clause}"
