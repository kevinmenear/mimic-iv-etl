from pyspark.sql import DataFrame

from pyspark.sql.functions import col
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler

import numpy as np

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, MinMaxScaler, LabelEncoder
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, roc_auc_score
from sklearn.metrics import confusion_matrix

from imblearn.over_sampling import SMOTE
from imblearn.under_sampling import RandomUnderSampler
from imblearn.pipeline import Pipeline

import xgboost as xgb

import seaborn as sns
import matplotlib.pyplot as plt

class MLUtils:
    def __init__(self, data: DataFrame):
        self.data = data
        
        self.features = None # List of input features
        self.target = None # List of target features
        
        self.train_data = None
        self.eval_data = {'val': None, 'test': None}
        
        self.model = None
        
        self.predictions = {'val': None, 'test': None}

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


    def clean_data(self, columns=None, verbose=False):
        """
        Removes rows from the DataFrame where specified columns have missing values and logs the count of dropped rows.
    
        Parameters:
        - columns (list, optional): A list of column names to check for missing values. If None, checks all columns.
        """
        if columns is None:
            # If no columns are specified, consider all feature and target columns
            columns = self.features + [self.target]
    
        # Count rows before cleaning
        if verbose: 
            initial_row_count = self.data.count()
    
        # Perform cleaning
        self.data = self.data.na.drop(subset=columns, how='any')
    
        if verbose: 
            # Count rows after cleaning
            final_row_count = self.data.count()
            # Calculate and log the number of rows dropped
            dropped_rows = initial_row_count - final_row_count
            print(f"Number of rows dropped: {dropped_rows}")
    
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

        elif selection_type == 'correlation':
            # Calculate correlation and filter features
            if self.target is None:
                raise ValueError("Target feature needs to be set with set_target method before calculating feature correlation.")
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
        train_data, remaining_data = train_test_split(self.data.toPandas(), test_size=(val_ratio + test_ratio), random_state=42)
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
    
        self.train_data = train_data
        self.eval_data['val'] = val_data
        self.eval_data['test'] = test_data

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

    def train_regression_model(self):
        xg_reg = xgb.XGBRegressor()
        xg_reg.fit(self.train_data[self.features], self.train_data[self.target])
        self.model = xg_reg

    def train_classification_model(self, smote=True, undersample_factor=0, verbose=False):
        """
        Trains a classification model using XGBoost, with optional SMOTE and undersampling.
        
        Parameters:
        - smote (bool): Whether to apply SMOTE for oversampling the minority class.
        - undersample_factor (float): Factor for undersampling the majority class. Ranges from 0 (no undersampling) to 1 (undersample until classes are balanced).
        - verbose (bool): Whether to print sampling strategy feedback.
        """
        # Split the data
        X_train, y_train = self.train_data[self.features], self.train_data[self.target]

        # Define model
        self.model = xgb.XGBClassifier()

        # Handle imbalanced data
        steps = []
        if undersample_factor > 0:
            class_counts = y_train.value_counts()
            minority_class_count = class_counts.min()
            majority_class_count = class_counts.max()
            current_ratio = minority_class_count/majority_class_count
            if verbose:
                print(f'Minority Class Count: {minority_class_count}, Majority Class Count: {majority_class_count}')
                print(f'Minority/Majority Ratio: {current_ratio:.4f}')
            # Apply undersampling strategy
            undersample_ratio = current_ratio * (1 - undersample_factor) + undersample_factor
            if verbose:
                print(f'Undersampling Majority. New Minority/Majority Ratio: {undersample_ratio:.4f}')
            under_sampler = RandomUnderSampler(sampling_strategy=undersample_ratio)
            steps.append(('under', under_sampler))

        if undersample_factor < 1 and smote:
            # Apply SMOTE for oversampling
            smote_sampler = SMOTE()
            steps.append(('smote', smote_sampler))

        # Create pipeline with resampling and model
        pipeline = Pipeline(steps=steps + [('model', self.model)])

        # Train the model
        pipeline.fit(X_train, y_train)

        # Store trained model
        self.model = pipeline

    def evaluate_classification_model(self, eval_type='val'):
        """
        Evaluates the classification model on the test set.

        Returns:
            dict: A dictionary containing key evaluation metrics for regression.
        """
        
        self.predictions[eval_type] = self.model.predict(self.eval_data[eval_type][self.features])
        
        act = self.eval_data[eval_type][self.target]
        pred = self.predictions[eval_type]
        
        accuracy = accuracy_score(act, pred)
        precision = precision_score(act, pred)
        recall = recall_score(act, pred)
        f1 = f1_score(act, pred)
        auc_roc = roc_auc_score(act, pred)

        # Return evaluation metrics
        return {'accuracy': accuracy, 'precision': precision, 'recall': recall, 'f1': f1, 'auc_roc': auc_roc}

    def evaluate_regression_model(self, eval_type='val'):
        """
        Evaluates the regression model on the test set.

        Returns:
            dict: A dictionary containing key evaluation metrics for regression.
        """
        # Assuming self.model is the trained regression model and self.data['test'] is the test set
        self.predictions[eval_type] = self.model.predict(self.data['test'][self.features])

        act = self.eval_data[eval_type][self.target]
        pred = self.predictions[eval_type]
        
        # Calculate evaluation metrics
        mse = mean_squared_error(act, pred)
        mae = mean_absolute_error(act, pred)
        r2 = r2_score(act, pred)

        # Return evaluation metrics
        return {'mean_squared_error': mse, 'mean_absolute_error': mae, 'r2_score': r2}

    def display_confusion_matrix(self, eval_type='val'):
        """
        Displays a confusion matrix for the classification results.
    
        Parameters:
        - eval_type (str): The evaluation dataset type ('val' or 'test').
        """
        
        actual = self.eval_data[eval_type][self.target]
        predicted = self.predictions[eval_type]
    
        # Generate confusion matrix
        matrix = confusion_matrix(actual, predicted)
    
        # Plotting using seaborn
        sns.heatmap(matrix, annot=True, fmt='d', cmap='Blues', 
                    xticklabels=['Predicted Negative', 'Predicted Positive'],
                    yticklabels=['Actual Negative', 'Actual Positive'])
        plt.ylabel('Actual')
        plt.xlabel('Predicted')
        plt.title(f"Confusion Matrix ({'Validation Data' if eval_type == 'val' else 'Test Data'})")
        plt.show()


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
