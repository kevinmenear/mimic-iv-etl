import numpy as np

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder

from pyspark.sql import DataFrame

from pyspark.sql.functions import col
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler


class MLPrep:
    def __init__(self, data: DataFrame):
        self.data = data
        # Additional initializations can be added here

    def clean_data(self, column_name):
        """
        Removes rows from the DataFrame where the specified column has missing values.

        Parameters:
        - column_name (str): The name of the column to check for missing values.
        """
        # Drop rows where the specified column has a missing (null) value
        self.data = self.data.na.drop(subset=[column_name])
        return self.data

    def select_features(self, features, selection_type, correlation_threshold=None, top_n=None):
        """
        Selects features based on the specified selection type.

        Parameters:
        - features (list): A list of features (column names).
        - selection_type (str): The type of feature selection ('subset' or 'correlation').
        - correlation_threshold (float, optional): The threshold for selecting features based on correlation.
        - top_n (int, optional): Select top n features based on correlation.

        Returns:
        - DataFrame: The DataFrame with selected features.
        """

        def process_correlation_matrix(corr_matrix, correlation_threshold=None, top_n=None):
            """
            Processes the correlation matrix to filter features.
        
            Parameters:
            - corr_matrix (numpy.ndarray): The correlation matrix.
            - correlation_threshold (float, optional): The threshold for selecting features.
            - top_n (int, optional): The number of top features to select based on correlation.
        
            Returns:
            - list: A list of indices representing the selected features.
            """
            selected_features = []
        
            if correlation_threshold is not None:
                # Select features based on correlation threshold
                for i in range(corr_matrix.shape[0]):
                    if np.abs(corr_matrix[i, i]) >= correlation_threshold:
                        selected_features.append(i)
        
            elif top_n is not None:
                # Select top n features based on absolute correlation values
                top_indices = np.argsort(-np.abs(np.diag(corr_matrix)))[:top_n]
                selected_features = list(top_indices)
        
            return selected_features
    
        if selection_type == 'subset':
            # Keep only the specified subset of features
            self.data = self.data.select([col(f) for f in features])

        elif selection_type == 'correlation':
            # Calculate correlation and filter features
            vector_col = "corr_features"
            assembler = VectorAssembler(inputCols=features, outputCol=vector_col)
            df_vector = assembler.transform(self.data).select(vector_col)

            # Get correlation matrix
            matrix = Correlation.corr(df_vector, vector_col)
            corr_matrix = matrix.collect()[0][0].toArray()

            # Process the correlation matrix to filter features
            selected_features = process_correlation_matrix(corr_matrix, correlation_threshold, top_n)
            self.data = self.data.select([col(f) for f in selected_features])

        return self.data

    def engineer_features(self):
        # Implement feature engineering steps
        pass

    def split_data(self, ratio=(0.7, 0.15, 0.15), normalization=None, standardization=None, encoding=None):
        """
        Splits the data into training, validation, and test sets, with options to normalize, standardize, or encode columns.
    
        Parameters:
        - ratio (tuple): Split ratio for training, validation, and test sets.
        - normalization (list of str, optional): Columns to normalize.
        - standardization (list of str, optional): Columns to standardize.
        - encoding (list of str, optional): Columns to label encode.
    
        Returns:
        - dict: A dictionary containing the split datasets.
        """
        train_ratio, val_ratio, test_ratio = ratio
        if not (0 < train_ratio < 1 and 0 < val_ratio < 1 and 0 < test_ratio < 1):
            raise ValueError("Ratios must be between 0 and 1")
    
        # Split the data
        train_data, remaining_data = train_test_split(self.data, test_size=(val_ratio + test_ratio), random_state=42)
        val_data, test_data = train_test_split(remaining_data, test_size=test_ratio/(val_ratio + test_ratio), random_state=42)
            
        # Normalize/standardize/encode if specified
        if normalization:
            scaler = MinMaxScaler()
            self.log_changes(f'Normalized {normalization} columns.')
            for col in normalization:
                scaler.fit(train_data[[col]])
                train_data[col] = scaler.transform(train_data[[col]])
                val_data[col] = scaler.transform(val_data[[col]])
                test_data[col] = scaler.transform(test_data[[col]])
    
        if standardization:
            scaler = StandardScaler()
            self.log_changes(f'Standardized {standardization} columns.')
            for col in standardization:
                scaler.fit(train_data[[col]])
                train_data[col] = scaler.transform(train_data[[col]])
                val_data[col] = scaler.transform(val_data[[col]])
                test_data[col] = scaler.transform(test_data[[col]])

        if encoding:
            encoder = LabelEncoder()
            self.log_changes(f'Encoded {encoding} columns.')
            for col in encoding:
                encoder.fit(train_data[[col]])
                train_data[col] = encoder.transform(train_data[[col]])
                val_data[col] = encoder.transform(val_data[[col]])
                test_data[col] = encoder.transform(test_data[[col]])
    
        return {'train': train_data, 'validation': val_data, 'test': test_data}

    def export_data(self, filename='exported_data'):
        """
        Exports the processed dataset to a file in the ../data/processed/ directory.
    
        Parameters:
        - filename (str): Name of the file for the exported data.
    
        The data is exported in csv.bz2 format.
        """
        filename += '.csz.bz2'
    
        filepath = os.path.join('../data/processed/', filename)
        self.data.to_csv(filepath, index=False, compression='bz2')
    
        print(f"Data exported successfully to {filepath}")

    def log_changes(self, change_description):
        """
        Logs a description of the changes made to the data.

        Parameters:
        - change_description (str): Description of the change.
        """
        log_entry = {
            'timestamp': datetime.datetime.now().isoformat(),
            'description': change_description
        }

        with open('../data/processed/data_processing_log.json', 'a') as log_file:
            log_file.write(json.dumps(log_entry) + '\n')

        print("Change logged successfully.")

    # Additional methods and utilities can be added as needed
