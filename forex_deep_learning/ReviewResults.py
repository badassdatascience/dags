import json
import pickle

import numpy as np
import matplotlib.pyplot as plt

from tensorflow.keras.models import model_from_json

class ReviewResults():

    def __init__(
        self,
        training_run_id,
        training_data_source_id,
        application_root_diretory,
        training_data_source_root_directory,
    ):
        self.application_root_directory = application_root_directory
        self.training_run_id = training_run_id
        self.training_data_source_root_directory = training_data_source_root_directory
        self.training_data_source_id = training_data_source_id
        
        self.model_training_output_directory = application_root_directory + '/output/'
        self.filename_config_json = model_training_output_directory + '/' + training_run_id + '_lstm_regressor_config.json'
        self.filename_model_json = model_training_output_directory + '/' + training_run_id + '_model_regressor.json'
        self.filename_model_final_weights = model_training_output_directory + '/' + training_run_id + '_final_weights_regressor.pickled'
        self.filename_history = model_training_output_directory + '/' + training_run_id + '_final_history_regressor.pickled'
        self.filename_train_val_test_data = training_data_source_root_directory + '/full_train_val_test_' + training_data_source_id + '.pickled'        

    def load_training_run_config(self):
        with open(self.filename_config_json) as fff:
            self.config = json.load(fff)

    def load_model(self):
        with open(self.filename_model_json, 'r') as json_file:
            self.loaded_model_json = json_file.read()

        # Create a new model from the JSON
        self.model = model_from_json(loaded_model_json)

        # Load the weights into the new model
        self.model.load_weights(self.filename_model_final_weights)

    def load_training_history(self):
        with open(self.filename_history, 'rb') as fff:
            self.history = pickle.load(fff)

    def load_train_val_test_data(self):
        with open(self.filename_train_val_test_data, 'rb') as fff:
            self.train_val_test_dict = pickle.load(fff)

    def predict(self, X_set_name = 'test'):
        self.y_predicted = model.predict(self.train_val_test_dict[X_set_name]['X'])
    
    def plot_basic_loss(
        self,
        metric_base = 'loss',
        ylabel = 'Loss',
    ):
        epochs = np.arange(1, len(self.history[metric_base]) + 1)

        plt.figure()
        plt.plot(epochs, self.history[metric_base], '-.', label = ylabel)
        plt.plot(epochs, self.history['val_' + metric_base], '-.', label = 'Validation ' + ylabel)
        plt.xlabel('Training Epoch')
    
        plt.ylabel(ylabel)
        plt.title('Training Run: ' + self.training_run_id.split('----')[-1] + '\nBatch Size = ' + str(self.config['batch_size']))
    
        plt.legend()
        plt.tight_layout()
        plt.show()
        plt.close()

    #
    # FIX THIS!
    #
    def plot_a_forecast(X, y_known, y_predicted, y_forward, model, index = 20, predictor_line = 0):

        indices_X = np.arange(0, X[index].shape[0])
        indices_y_forward = np.arange(len(indices), len(indices) + len(y_forward[index, :]))

        X_mean = np.mean(X[index, :, predictor_line])

        y_predicted_min = y_predicted[index, 0]
        y_predicted_mean = y_predicted[index, 1]  # check index of n = 1
        y_predicted_median = y_predicted[index, 2]  # check index of n = 1
        y_predicted_max = y_predicted[index, 3]
    
        plt.figure()
        plt.plot(indices_X, X[index, :, predictor_line])
        plt.plot(indices_X, [X_mean] * len(indices))
    
        plt.plot(indices_y_forward, y_forward[index, :])
        plt.plot(indices_y_forward, [y_predicted_min] * len(indices_y_forward))
        plt.plot(indices_y_forward, [y_predicted_mean] * len(indices_y_forward))
        plt.plot(indices_y_forward, [y_predicted_median] * len(indices_y_forward))
        plt.plot(indices_y_forward, [y_predicted_max] * len(indices_y_forward))
    
        plt.show()
        plt.close()



