import numpy as np

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, MinMaxScaler, LabelEncoder
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, roc_auc_score
from imblearn.over_sampling import SMOTE
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier


from pyspark.sql import DataFrame

from pyspark.sql.functions import col
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler


class MLUtils:
    def __init__(self, data: DataFrame):
        self.data = data
        self.target = None
        self.features = None

    def set_target(self, target: str):
        """
        Sets the target variable for the machine learning model.
    
        Parameters:
        target (str): The name of the column to be used as the target variable.
        """
        if target not in self.data.columns:
            raise ValueError(f"Target column '{target}' not found in the dataset.")
        self.target = target
    
    def set_features(self, features: list):
        """
        Sets the features for the machine learning model.
    
        Parameters:
        features (list): A list of column names to be used as features.
        """
        for feature in features:
            if feature not in self.data.columns:
                raise ValueError(f"Feature column '{feature}' not found in the dataset.")
        self.features = features


    def clean_data(self, column_name):
        """
        Removes rows from the DataFrame where the specified column has missing values.

        Parameters:
        - column_name (str): The name of the column to check for missing values.
        """
        # Drop rows where the specified column has a missing (null) value
        self.data = self.data.na.drop(subset=[column_name])
        return self.data
        

    def select_features(self, features, selection_type='subset', correlation_threshold=None, top_n=None):
        """
        Selects features based on correlation with the target variable.
        
        Parameters:
        - features (list): A list of features (column names).
        - target (str): The target variable.
        - correlation_threshold (float, optional): The threshold for selecting features based on correlation.
        - top_n (int, optional): Select top n features based on correlation.
    
        Returns:
        - DataFrame: The DataFrame with selected features.
        """
        if selection_type == 'subset':
            # Keep only the specified subset of features
            self.set_features(features)
            self.data = self.data.select([col(f) for f in features])

        elif selection_type == 'correlation':
            # Calculate correlation and filter features
            if self.target is None:
                raise ValueError("Target feature needs to be set with set_target method before calculating feature correlation."
            correlated_features = []
        
            for feature in features:
                # Create a vector containing the feature and the target variable
                vector_col = f"corr_{feature}"
                assembler = VectorAssembler(inputCols=[feature, self.target], outputCol=vector_col)
                df_vector = assembler.transform(self.data).select(vector_col)
        
                # Calculate correlation
                matrix = Correlation.corr(df_vector, vector_col).collect()[0][0].toArray()
                corr_value = matrix[0][1]  # Correlation between feature and target
        
                # Check if the feature meets the correlation criteria
                if (correlation_threshold and abs(corr_value) >= correlation_threshold) or (top_n is not None):
                    correlated_features.append((feature, corr_value))
        
            # If top_n is specified, select the top n features
            if top_n:
                correlated_features.sort(key=lambda x: abs(x[1]), reverse=True)
                correlated_features = correlated_features[:top_n]
        
            # Select the features that met the criteria
            selected_feature_names = [feature for feature, _ in correlated_features]
            self.set_features(selected_feature_names)
            self.data = self.data.select([col(f) for f in selected_feature_names])
    
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

    def evaluate_classification_model(self):
        """
        Evaluates the classification model on the test set.

        Returns:
            dict: A dictionary containing key evaluation metrics for regression.
        """
        predictions = self.model.predict(self.data['test'][self.features])
        accuracy = accuracy_score(self.data['test'][self.target], predictions)
        precision = precision_score(self.data['test'][self.target], predictions)
        recall = recall_score(self.data['test'][self.target], predictions)
        f1 = f1_score(self.data['test'][self.target], predictions)
        auc_roc = roc_auc_score(self.data['test'][self.target], predictions)

        # Return evaluation metrics
        return {'accuracy': accuracy, 'precision': precision, 'recall': recall, 'f1': f1, 'auc_roc': auc_roc}

    def evaluate_regression_model(self):
        """
        Evaluates the regression model on the test set.

        Returns:
            dict: A dictionary containing key evaluation metrics for regression.
        """
        # Assuming self.model is the trained regression model and self.data['test'] is the test set
        predictions = self.model.predict(self.data['test'][self.features])
        
        # Calculate evaluation metrics
        mse = mean_squared_error(self.data['test'][self.target], predictions)
        mae = mean_absolute_error(self.data['test'][self.target], predictions)
        r2 = r2_score(self.data['test'][self.target], predictions)

        # Return evaluation metrics
        return {'mean_squared_error': mse, 'mean_absolute_error': mae, 'r2_score': r2}


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



# Subclasses of MLPrep for various modeling goals

class ReadmissionRiskPrediction(MLUtils):
    def __init__(self, data):
        super().__init__(data)
        # Initialize target variable and feature selection
        self.target = None
        self.features = None


    # Add model goal-specific methods
    def feature_engineering_for_readmission(self):
        # Specific feature engineering steps
        pass

    def train_model(self):
        # Split the data
        split_data = self.split_data()

        # Example: Logistic Regression Model
        clf = LogisticRegression()

        # Handle imbalanced data
        smote = SMOTE()
        X_resampled, y_resampled = smote.fit_resample(split_data['train'][self.features], split_data['train'][self.target])

        # Training the model
        clf.fit(X_resampled, y_resampled)

        # Storing trained model for future use
        self.model = clf

    def evaluate_model(self):
        # Logic to evaluate the readmission risk model
        pass

