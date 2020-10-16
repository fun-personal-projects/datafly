from pycaret.regression import *

# Test
#
# data = pd.read_csv("./example/db_100.csv")
# print(data.head())
#
# f1 = setup(data, target = 'disease', session_id=123, log_experiment=True,
#            experiment_name='test', html = False, silent = True)
#
# print("Finding best model for this ")
# best_model = compare_models()
#
# m1 = create_model('llar')
#
# m2 = create_model('br')
#
# m3 = create_model('tr')
#
# # Tuning
# print("Training the models")
#
# m3_t = tune_model(m3)
# m2_t = tune_model(m2)
# m1_t = tune_model(m1)
#

# Final function


def main_ret(data):
    # data = pd.read_csv(data)
    print(data.head())

    f1 = setup(data, target='disease', session_id=123, log_experiment=False,
               experiment_name='test', html=False, silent=True)
    # compare_models(fold=5) 

    print("Finding best model for this ")
    m1 = create_model('lasso') #lasso

    m2 = create_model('en') #elastic

    m3 = create_model('lightgbm')

    # Tuning
    print("Training the models")

    m3_t = tune_model(m3)
    m2_t = tune_model(m2)
    print("Best model")
    m1_t = tune_model(m1)

# main_ret(pd.read_csv("./example/db_100.csv"))
# main_ret(pd.read_csv("./example/db_100_3_anon.csv"))
