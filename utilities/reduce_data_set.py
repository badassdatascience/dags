#
# load modules
#
import numpy as np
import pickle

#
# temp
#
n_step = 5
run_id = '309457bc-a227-4332-8c0b-2cf5dd38749c'
run_dir = '/home/emily/Desktop/projects/test/badass-data-science/badassdatascience/forecasting/deep_learning/pipeline_components/output/queries'
filepath = run_dir + '/full_train_val_test_' + run_id + '.pickled'

#
# load data
#
with open(filepath, 'rb') as fff:
    rdict = pickle.load(fff)


stuff = {}
    
for tvt_item in rdict.keys():
    stuff[tvt_item] = {}
    data_item_list = rdict[tvt_item].keys()
    for data_item in data_item_list:
        Q = rdict[tvt_item][data_item]
        n_samples = Q.shape[0]

        indices = np.arange(0, n_samples, n_step)
        
        if len(Q.shape) > 2:
            R = Q[indices, :, :]
        else:
            R = Q[indices, :]
    
        stuff[tvt_item][data_item] = R




import pprint as pp
print()
pp.pprint(stuff['train']['X'].shape)
print()
pp.pprint(stuff['val']['X'].shape)
print()
pp.pprint(stuff['test']['X'].shape)
print()
