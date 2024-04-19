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
from sklearn.feature_selection import RFE

from imblearn.over_sampling import SMOTE
from imblearn.under_sampling import RandomUnderSampler
from imblearn.pipeline import Pipeline

import xgboost as xgb

import optuna

import shap

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

    def select_features_with_rfe(self, model_type='classification', n_features_to_select=None, step=1, verbose=False):
        """
        Feature selection using Recursive Feature Elimination (RFE) based on specified model type (classifier or regressor).
    
        Parameters:
        - model_type (str): Type of model ('classification' or 'regression') for which to perform feature selection.
        - n_features_to_select (int, optional): The number of features to select. If None, half of the features are selected.
        - step (int or float, optional): The number of features to consider removing at each iteration.
        - verbose (bool, optional): Whether to print the process details.
        """
        # Ensure target and features are already set
        if self.target is None or self.features is None:
            raise ValueError("Target and features must be set before feature selection.")
    
        # Choose the model based on the specified type
        if model_type == 'classification':
            model = xgb.XGBClassifier()
        elif model_type == 'regression':
            model = xgb.XGBRegressor()
        else:
            raise ValueError("Invalid model type specified. Choose 'classification' or 'regression'.")
    
        # Define RFE with the chosen model
        rfe = RFE(estimator=model, n_features_to_select=n_features_to_select, step=step, verbose=verbose)
    
        # Fit RFE
        self.data[self.features + [self.target]] = self.data[self.features + [self.target]].dropna()  # Ensure no NaNs
        X = self.data[self.features]
        y = self.data[self.target]
        rfe.fit(X, y)
    
        # Update features to the selected features
        self.features = [f for f, selected in zip(self.features, rfe.support_) if selected]
    
        # Log the feature selection
        self.log_changes(f"Selected features with RFE: {self.features}")
    
        return self.features


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

    def train_regression_model(self, params=None):
        if params:
            xg_reg = xgb.XGBRegressor(**params)
        else:
            xg_reg = xgb.XGBRegressor()
        xg_reg.fit(self.train_data[self.features], self.train_data[self.target])
        self.model = xg_reg

    def train_classification_model(self, smote=True, undersample_factor=0, params=None, verbose=False):
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
        if params:
            self.model = xgb.XGBClassifier(**params)
        else:
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
        Evaluates the classification model on the validation or test set.

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
        Evaluates the regression model on the validation or test set.

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


    def optimize_with_optuna(self, model_type='classification', n_trials=100, storage_url='sqlite:///optuna_study.db'):
        """
        Optimize hyperparameters using Optuna for the specified model type. This method sets up an optimization trial with 
        Optuna to search for the best hyperparameters that maximize or minimize the specified evaluation metric. The function 
        uses a storage mechanism to save all trials for resuming or reviewing in future sessions.
    
        Parameters:
        - model_type (str): Specifies the type of model to optimize. Acceptable values are 'classification' or 'regression'.
        - n_trials (int): The number of trials Optuna should run to optimize the hyperparameters.
        - storage_url (str): URL to the database storage used to save the Optuna study's state. Format is 'sqlite:///file_path'.
    
        Returns:
        - best_params (dict): A dictionary containing the hyperparameters that yielded the best performance metric 
            (accuracy for classification or mean squared error for regression) during the trials.
    
        Raises:
        - ValueError: If an invalid 'model_type' is specified.
    
        Example Usage:
        - best_params = optimize_with_optuna(model_type='regression', n_trials=50, storage_url='sqlite:///my_optuna.db')
        """
    
        def objective(trial):
            # Define the hyperparameter space
            params = {
                'n_estimators': trial.suggest_int('n_estimators', 50, 400),
                'max_depth': trial.suggest_int('max_depth', 3, 20),
                'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.2, log=True),
                'subsample': trial.suggest_float('subsample', 0.5, 1.0),
                'colsample_bytree': trial.suggest_float('colsample_bytree', 0.5, 1.0),
                'lambda': trial.suggest_float('lambda', 1e-8, 10.0, log=True),
                'alpha': trial.suggest_float('alpha', 1e-8, 10.0, log=True),
                'min_child_weight': trial.suggest_int('min_child_weight', 1, 10),
                'eta': trial.suggest_float('eta', 0.01, 0.1),
                'gamma': trial.suggest_float('gamma', 0.0, 1.0)
            }

    
            # Initialize the model
            if model_type == 'classification':
                smote = trial.suggest_categorical('smote', [True, False])
                undersample_factor = trial.suggest_float('undersample_factor', 0.0, 1.0)
                self.train_classification_model(smote, undersample_factor, params=params)
                results = self.evaluate_classification_model()
                metric = results['accuracy']
            elif model_type == 'regression':
                self.train_regression_model(params=params)
                results = self.evaluate_regression_model()
                metric = results['mean_squared_error']
            else:
                raise ValueError("Invalid model type specified. Choose 'classification' or 'regression'.")

            return metric

        if model_type == 'classification':
            direction='maximize'
        elif model_type == 'regression':
            direction='minimize'
        study = optuna.create_study(direction=direction, storage=storage_url, load_if_exists=True)
        study.optimize(objective, n_trials=n_trials)
    
        best_params = study.best_trial.params
        print(f"Best trial parameters: {best_params}")
        

        # Train the model with the best parameters
        if model_type == 'classification':
            smote = best_params.pop('smote')
            undersample_factor = best_params.pop('undersample_factor')
            self.train_classification_model(smote, undersample_factor, params=best_params)
        else:
            self.train_regression_model(params=best_params)
        print('Model trained with best trial parameters')
        
        return best_params
        

    def compute_shap_values(self):
        """
        Compute SHAP values to explain the model's decisions. This function initializes a SHAP explainer with the trained model and 
        calculates SHAP values for the training data.
    
        Raises:
        - ValueError: If the model is not trained prior to calling this function.
    
        Returns:
        - shap_values: An object containing SHAP values for each feature across the training data. Each row in the SHAP values object
          sums up to the difference between the model output for that row and the base value output by the model for the dataset.
    
        Example:
        - shap_values = compute_shap_values()
    
        Note:
        - Ensure that the model is trained and the data used for computing SHAP values is representative of the model's application context.
        """
        # Ensure the model is trained
        if self.model is None:
            raise ValueError("Model not trained. Train the model before computing SHAP values.")
    
        # Extract the actual model from the pipeline if it's wrapped in one
        actual_model = self.model.steps[-1][1] if hasattr(self.model, 'steps') else self.model
    
        explainer = shap.Explainer(actual_model)
        self.shap_values = explainer(self.train_data[self.features])
    
        return self.shap_values


    def visualize_shap_values(self, plot_type='bar', sample_number=0, feature_name=None):
        """
        Visualize SHAP values in various formats. This function provides different plots to represent the SHAP values depending
        on the plot type specified.
    
        Parameters:
        - shap_values: The SHAP values object computed from the compute_shap_values function.
        - plot_type (str): Type of plot to display SHAP values. Acceptable values include 'bar', 'waterfall', 'dependence', and 'bee_swarm'.
          - 'bar' plot shows the average impact of each feature on model output.
          - 'waterfall' shows the contribution of each feature to a single prediction, specified by sample_number.
          - 'dependence' plots the effect of a single feature, specified by feature_name, across the whole dataset.
          - 'bee_swarm' plots the distribution of impacts each feature has across all samples.
        - sample_number (int): The index of the sample to plot in the 'waterfall' plot.
        - feature_name (str): The name of the feature to plot in the 'dependence' plot.
    
        Raises:
        - ValueError: If an incorrect plot type is provided.
    
        Example:
        - visualize_shap_values(shap_values, plot_type='bar')
    
        Note:
        - Ensure that SHAP values are computed using compute_shap_values before calling this function.
        """
        # Check if the correct plot type is provided
        if plot_type not in ['bar', 'waterfall', 'dependence', 'bee_swarm']:
            raise ValueError("Invalid plot type provided. Choose 'bar', 'waterfall', 'dependence', or 'bee_swarm'.")
    
        # Visualize the SHAP values
        if plot_type == 'bar':
            shap.plots.bar(self.shap_values)
        elif plot_type == 'waterfall':
            shap.plots.waterfall(self.shap_values[sample_number])
        elif plot_type == 'dependence':
            if feature_name is None:
                raise ValueError("Feature name must be specified for 'dependence' plot.")
            shap.plots.scatter(self.shap_values[:, feature_name])
        elif plot_type == 'bee_swarm':
            shap.plots.beeswarm(self.shap_values)
    
        return
        

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
