import pickle
import numpy as np

def shuffle_indices(indices_train, indices_val, indices_test):
    np.random.shuffle(indices_train)
    np.random.shuffle(indices_val)
    np.random.shuffle(indices_test)
    return indices_train, indices_val, indices_test

def test_shuffle(indices_train, indices_val, indices_test):
    print()
    print('Shuffle test:')
    print()
    print(min(indices_train), max(indices_train), indices_train[0:10])
    print(min(indices_val), max(indices_val), indices_val[0:10])
    print(min(indices_test), max(indices_test), indices_test[0:10])
    print()
    
def train_val_test_split(the_dict):

    # We assume for now that the contents come chronologically sorted
    # based on past investigation. Confirming this before spliting
    # and shuffling might be a good itea.

    
    #
    # temp
    #
    ratio_train = 0.70
    ratio_val = 0.15
    ratio_test = 0.15
    shuffle_it = True
    random_seed = 20
    verbose = False

    #
    # set random seed
    #
    np.random.seed(random_seed)
    
    #
    # load data
    #
    with open(the_dict['back_to_pandas_save_path'], 'rb') as fff:
        dict_full_Xy = pickle.load(fff)

    #
    # QA
    #
    QA_list = []
    for key in dict_full_Xy.keys():
        QA_list.append(int(dict_full_Xy[key].shape[0]))
    if verbose:
        print()
        print('QA passed? ', min(QA_list) == max(QA_list))
        print()

        print(dict_full_Xy.keys())
        print()

    
    #
    # split
    #
    n_samples = QA_list[0]
    indices = np.arange(0, n_samples)

    cutoff_train = np.int32(np.round(n_samples * ratio_train))
    cutoff_val = np.int32(np.round(n_samples * ratio_val)) + cutoff_train
    cutoff_test = np.int32(np.round(n_samples * ratio_val)) + cutoff_val

    indices_train = np.arange(0, cutoff_train)
    indices_val = np.arange(cutoff_train, cutoff_val)
    indices_test = np.arange(cutoff_val, n_samples)

    indices_dict = {
        'train' : indices_train,
        'val' : indices_val,
        'test' : indices_test,
    }

    
    #
    # shuffle
    #
    if shuffle_it:
        indices_train, indices_val, indices_test = shuffle_indices(
            indices_train,
            indices_val,
            indices_test
        )
        if verbose:
            test_shuffle(indices_train, indices_val, indices_test)
    
    #
    # QA #2
    #
    if verbose:
        print()
        for item in [indices_train, indices_val, indices_test]:
            print(item[0], item[-1], ((item[-1] - item[0]) / n_samples))
        print()

    #
    # conduct full split
    #
    result_dict = {}
    columns_list = list(dict_full_Xy.keys())
    for key in indices_dict:
        if not key in result_dict:
            result_dict[key] = {}
        for column in columns_list:
            result_dict[key][column] = dict_full_Xy[column][indices_dict[key]]


    #
    # QA (results dictionary)
    #
    if verbose:
        print()
        import pprint as pp
        pp.pprint(result_dict)
        print()

    verbose = True
        
    #
    # QA (more testing the results dictionary)
    #
    if verbose:
        print()
        passes_the_test = True
        for key in result_dict.keys():
            print()
            test_value_list = []
            for column in result_dict[key].keys():
                print(key, column, result_dict[key][column].shape)
                test_value_list.append(result_dict[key][column].shape[0])

            if min(test_value_list) != max(test_value_list):
                passes_the_test = False
        print()
        print('Passes?', passes_the_test)
        print()
        
    
    return {'booger', 'booger'}
    
