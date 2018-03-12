import numpy as np
import pandas as pd
from sklearn.cross_validation import train_test_split
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import accuracy_score
from sklearn import tree
import sklearn as sk
import collections
import pydotplus
from sklearn.externals import joblib
from sklearn.utils import resample
import sys
import os
output_dir = '/home/strikermx/output_dir/model_'+sys.argv[1]
data_dir = '/home/strikermx/data_dir/model_'+sys.argv[1]
if not os.path.exists(output_dir):
    os.makedirs(output_dir)
#
#load data to dataframe
df = pd.read_csv(data_dir + '/training_' + sys.argv[1] + '.dat', sep='|')
#downsampling dataset to maintain 0.5 label prior
df_majority = df[df.label==0]
df_minority = df[df.label==1]
df_minority.count()
df_majority_downsampled = resample(df_majority, 
                                 replace=True,     # sample with replacement
                                 n_samples=len(df_minority.index),    # to match majority class
                                 random_state=123) # reproducible results
balanced_df = pd.concat([df_majority_downsampled, df_minority])
#write file
balanced_df.to_csv(data_dir + '/train_up20bl_' + sys.argv[1] + '.dat', sep='|')

# convert label data to y axis
Y = balanced_df.values[:,88]

#drop unable to use column and label column
balanced_df.drop(['analytic_id', 'label','voice_pay_per_use_p3','voice_quota_minute_p3'], axis=1, inplace=True)

# convert features data to x axis
X = balanced_df.values[:, :]

#convert type of X and Y
X = X.astype(np.float32)
Y = Y.astype(np.int)

#train model
clf_gini = DecisionTreeClassifier(criterion = "gini", max_leaf_nodes = 100, max_depth = 18)
clf_gini.fit(X, Y)

#save model to file
joblib.dump(clf_gini, filename)

#make fitted prediction
y_pred = clf_gini.predict(X)

#get all model decision node
node = clf_gini.tree_.apply(X)

#make prob fitted prediction
prob = clf_gini.predict_proba(X)

#convert to dataframe
probdf = pd.DataFrame(prob)
predictpd = pd.DataFrame(y_pred)
nodepd = pd.DataFrame(node)

#renaming dataframe column
predictpd.columns = ['predict']
nodepd.columns = ['leaf_node']
probdf.columns = ['prob_0','prob_1']

#join probdf and nodepd dataframe
probdf = pd.concat([probdf,nodepd], axis=1)

#make feature dict from balanced_df
data_feature_names = list(balanced_df.columns.values)
feature_dict = dict(enumerate(data_feature_names))

#get sample analytic_id
sample = df['analytic_id']
sampledf = pd.DataFrame(sample)
analytic_node = sampledf.join(predictpd)

#re-open balanced_df to get label column
balanced_df = pd.read_csv(data_dir + '/train_up20bl_' + sys.argv[1] + '.dat', sep='|')
output = balanced_df['label']
outputdf = pd.DataFrame(output)

#join predicted dataframe with df of analytic_id and label data
analytic_node = analytic_node.join(outputdf)
analytic_node = analytic_node.join(probdf)

#take only where model predict 1
fitted_prob = analytic_node.loc[analytic_node['predict'] == 1]

#save output to csv
fitted_prob.to_csv(output_dir + '/fitted_prob_' + sys.argv[1] + '.csv', sep=',')

#get only leaf node id and predicting prob of 1 to new
new = fitted_prob[['leaf_node', 'prob_1']].copy()

#group by leaf node id and calculate mean prob score
meanprob = new.groupby(['leaf_node']).mean()

#get count of sample in each leaft_node
pd.DataFrame({'count' : new.groupby( [ "leaf_node"] ).size()}).reset_index()
countn = new.groupby(['leaf_node']).size().reset_index(name='counts')
meanprob = new.groupby(['leaf_node'])['prob_1'].mean().reset_index(name='mean_prob')
new_df = pd.merge(meanprob, countn,  how='left', left_on=['leaf_node'], right_on = ['leaf_node'])

#sort mean_prob to look for best fitted decision path
new_df = new_df.sort_values(['mean_prob', 'counts'], ascending=[False, False])

#save output to csv
new_df.to_csv(output_dir + '/leafNode_meanProb_' + sys.argv[1] + '.csv', sep=',')

#get all nodes and it feature and match with feature dict
nodes = clf_gini.tree_.__getstate__()['nodes']
nodedf = pd.DataFrame(nodes)
nodedf = nodedf.replace({"feature": feature_dict})

#save node feature to csv
nodedf.to_csv(output_dir + '/node_feature_' + sys.argv[1] + '.csv', sep=',')

#select decision node with mean_prob of more than 80% fitted mean score
top80pn = new_df.loc[new_df['mean_prob'] >= 0.80]
test = list(top80pn.leaf_node.values)
topfit = fitted_prob.loc[fitted_prob['leaf_node'].isin(test)].groupby('leaf_node').head(1)
topfitind = topfit.index.values.tolist()

#convert node feature to sql like condition
sql_list = []
for tfi in topfitind:
    a = np.vstack((X[tfi,:]))
    path = clf_gini.decision_path(a.transpose())
    artp = path.toarray()
    decisionon = pd.DataFrame(artp)
    decisionon = decisionon.transpose()
    tmp_df = nodedf.loc[decisionon[0] == 1]
    tmp_df['node_id'] = tmp_df.index   
    sql_tmp =''
    last = tmp_df.irow(0)
    for i in range(1, tmp_df.shape[0]):
        if tmp_df.irow(i).node_id != 0:
            if not sql_tmp:
                if last.left_child == tmp_df.irow(i).node_id:
                    sql_tmp = sql_tmp + last.feature+' <= '+ "{0:.4f}".format(last.threshold)
                else:
                    sql_tmp = sql_tmp + last.feature+' > '+ "{0:.4f}".format(last.threshold)
            else:
                if last.left_child == tmp_df.irow(i).node_id:
                    sql_tmp = sql_tmp + ' AND ' + last.feature+' <= '+ "{0:.4f}".format(last.threshold)
                else:
                    sql_tmp = sql_tmp + ' AND ' + last.feature+' > '+ "{0:.4f}".format(last.threshold)
        if i < tmp_df.shape[0]:
            last = tmp_df.irow(i)
    sql_list.append(sql_tmp)
sqldf = pd.DataFrame({'sql': sql_list})
topfitr = topfit.reset_index(drop=True)
ressqldf = pd.concat([topfitr, sqldf], axis=1)
ressqldf.drop(['analytic_id', 'label','prob_0','prob_1'], axis=1, inplace=True)
top80leafsql = pd.merge(ressqldf, new_df,
                       how='left', on=['leaf_node'])
top80leafsql = top80leafsql.sort_values(['mean_prob', 'counts'], ascending=[False, False])

# save output to csv
top80leafsql.to_csv(output_dir + '/top80p_leafsql_' + sys.argv[1] + '.csv', sep=',')