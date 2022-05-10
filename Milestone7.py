from os import mkdir
from urllib.request import urlretrieve
from zipfile import ZipFile
import os
import pandas as pd
import numpy as np
import shutil
import streamlit as st


st.title('Milestone 7')
st.title('Step 1: URL')
# url = st.text_input('Enter URL of Test Data', '')
#st.write('The url you entered is', url)

with st.form(key='my_form'):
    urlinput = st.text_input(label='Enter URL of Test Data')
    submit_button = st.form_submit_button(label='Submit')
# https://www.dropbox.com/s/w15ev8i0noj85sa/4269819.zip?dl=1


# def retrieve_and_process():
#     st.session_state['loadingText'] = 'processing data......'

#     if os.path.exists('./DownloadedFile.zip'):
#         os.remove('./DownloadedFile.zip')
#     if os.path.exists('./DownloadFile'):
#         shutil.rmtree('./DownloadFile', ignore_errors=True)


if urlinput != '':
    url = urlinput
    csvname = url[-16:-9]
    # inputFile = "./DownloadedFile.zip"
    # outputDir = "DownloadedFile"
    INPUT_FILE_SAVE_DIR = "./DownloadedFile.zip"
    OUTPUT_DIR = "./DownloadFile/"
    # put zip file in local directory -- this takes a while.. file is big
    # urlretrieve(url, inputFile)
    urlretrieve(url, INPUT_FILE_SAVE_DIR)

    # unzip contents into output folder -- this takes a while.. file is big
    # with ZipFile(inputFile) as zipObj:
    #     zipObj.extractall(outputDir)
    with ZipFile(INPUT_FILE_SAVE_DIR) as zipObj:
        zipObj.extractall(OUTPUT_DIR)

    # Extract pic names in the folder
    file_list = os.listdir("./DownloadFile")
    file_list.sort()
    # remove .jpg
    new_list = [s.replace(".jpg", "") for s in file_list]

################ Milestone 2 ######################
    # input csv
    # read the csv file (put 'r' before the path string to address any special characters in the path, such as '\'). Don't forget to put the file name at the end of the path + ".csv"
    CustomerList = pd.read_csv("./liveCustomerList.csv")
    CustomerList['firstName'] = CustomerList['firstName'].str.upper()
    CustomerList['lastName'] = CustomerList['lastName'].str.upper()
    FraudList = pd.read_csv("./liveFraudList.csv")
    # ***change 1 to NA
    FraudList["fraudster"] = 'NA'

    # change custID list to Dataframe
    # ***separate custID and loginAcct
    jpg_custID = [s[0:4] for s in file_list]
    jpg_loginAcct = [s[-10:-4] for s in file_list]

    custID = pd.DataFrame(jpg_custID, columns=['custID'])
    loginAcct = pd.DataFrame(jpg_loginAcct, columns=['loginAcct'])

    # change datatype
    custID['custID'] = custID['custID'].astype(int)
    loginAcct['loginAcct'] = loginAcct['loginAcct'].astype(int)

    # left join table custID and customerlist
    custIDname = pd.merge(custID, CustomerList, on='custID', how='left')
    custIDnameFraud = pd.merge(custIDname, FraudList, on=[
        'firstName', 'lastName'], how='left')
    custIDnameFraud['fraudster'] = custIDnameFraud['fraudster'].fillna(0)

    # output fraud result
    output_fraud = custIDnameFraud[['custID', 'fraudster']]


################ Milestone 3 ######################
    # previous downloaded file format
    AcctInput = custID.copy()
    AcctInput['loginAcct'] = jpg_loginAcct

    # left join input table and customerlist
    custIDname_verify = pd.merge(
        AcctInput, CustomerList, on='custID', how='left')

    # input csv
    BankAcct = pd.read_csv("./liveBankAcct.csv")
    BankAcct['firstName'] = BankAcct['firstName'].str.upper()
    BankAcct['lastName'] = BankAcct['lastName'].str.upper()
    custIDname_verify = custIDname_verify.astype({"loginAcct": int})

    # left join with bankacct
    AcctName_verify = pd.merge(custIDname_verify, BankAcct,
                               left_on='loginAcct', right_on='bankAcctID', how='left')

    # add conditional rightAcctFlag column
    def f(AcctName_verify):
        if AcctName_verify['firstName_x'] == AcctName_verify['firstName_y'] and AcctName_verify['lastName_x'] == AcctName_verify['lastName_y']:
            val = 0
        else:
            val = 'NA'
        return val
    AcctName_verify['rightAcctFlag'] = AcctName_verify.apply(f, axis=1)

    # FraudTestOnput.csv
    output_verify = AcctName_verify[['custID', 'rightAcctFlag']]

################ Milestone 5 ######################
    # read startbalance & bankTransactions
    acctStBalan = pd.read_csv('./Milestone5Files/startBalance.csv')
    bankTrans = pd.read_csv('./Milestone5Files/bankTransactions.csv')

    # left join loginAcct & startBalanceInfo
    acctStBalan = loginAcct.merge(
        acctStBalan, left_on='loginAcct', right_on='bankAcctID', how='left')
    # left join loginAcct & bankTransactions, append acctIDTrans to acctStBalanInfo
    acctIDTrans = loginAcct.merge(
        bankTrans, left_on='loginAcct', right_on='bankAcctID', how='left')

    acctIDTrans = acctIDTrans.rename({'transAmount': 'amt'}, axis=1)

    # append acctIDTrans to acctStBalanInfo
    acctsBalanInfo = acctStBalan.append(acctIDTrans, ignore_index=True)
    acctsBalanInfo = acctsBalanInfo[acctsBalanInfo.amt > 200]

    second_max_date = acctsBalanInfo.sort_values(["bankAcctID", "date"], ascending=[1, 0]).groupby(
        "bankAcctID").agg({"date": lambda x: x.shift(-1).values[0]}).reset_index()
    second_max_date.columns = second_max_date.columns.str.replace(
        'date', 'second_max_date')
    max_date = acctsBalanInfo.groupby('bankAcctID')['date'].max().reset_index()
    max_date.columns = max_date.columns.str.replace('date', 'max_date')
    # left join second_max_date and max_date
    first_sec_max = max_date.merge(
        second_max_date, on='bankAcctID', how='left')

    pay_method = [
        ['2020-04-15', '2020-04-30', '2020-05-15'],
        ['2020-04-17', '2020-05-17', '2020-06-17'],
        ['2020-04-20', '2020-05-20', '2020-06-20'],
        ['2020-04-10', '2020-04-24', '2020-05-08'],
        ['2020-04-17', '2020-04-24', '2020-05-01'],
        ['2020-04-30', '2020-05-30', '2020-06-30'],
        ['2020-04-13', '2020-04-27', '2020-05-11'],
        ['2020-04-15', '2020-05-15', '2020-06-15'],
        ['2020-04-20', '2020-04-27', '2020-05-04'],
        ['2020-04-23', '2020-04-30', '2020-05-07'],
        ['2020-04-23', '2020-05-23', '2020-06-23'],
        ['2020-04-22', '2020-05-22', '2020-06-22'],
        ['2020-04-21', '2020-05-21', '2020-06-21'],
        ['2020-04-10', '2020-05-10', '2020-06-10'],
        ['2020-04-15', '2020-04-29', '2020-05-13'],
        ['2020-04-13', '2020-05-13', '2020-06-13'],
        ['2020-04-14', '2020-04-28', '2020-05-12'],
        ['2020-04-21', '2020-04-28', '2020-05-05'],
        ['2020-04-22', '2020-04-29', '2020-05-06'],
        ['2020-04-16', '2020-04-30', '2020-05-14'],
        ['2020-04-20', '2020-04-30', '2020-05-10'],
        ['2020-04-27', '2020-05-27', '2020-06-27'],
        ['2020-04-16', '2020-05-16', '2020-06-16'],
        ['2020-04-27', '2020-04-30', '2020-05-03'],
        ['2020-04-10', '2020-04-17', '2020-04-24'],
        # add
        ['2020-04-14', '2020-04-27', '2020-04-24'],
        ['2020-04-03', '2020-04-17', '2020-05-01'],
        ['2020-03-02', '2020-04-01', '2020-05-01'],  # not sure
        ['2020-04-07', '2020-04-21', '2020-05-05'],
        ['2020-03-23', '2020-04-13', '2020-05-04'],
        ['2020-04-09', '2020-04-23', '2020-05-07'],
        ['2020-03-31', '2020-04-15', '2020-04-30'],  # not sure
        ['2020-03-31', '2020-04-30', '2020-05-29'],  # not sure
        ['2020-04-06', '2020-04-20', '2020-05-04'],
        ['2020-04-08', '2020-04-22', '2020-05-06'],
        ['2020-03-06', '2020-04-06', '2020-05-06'],
        ['2020-03-30', '2020-04-13', '2020-04-27'],
        ['2020-04-01', '2020-04-08', '2020-04-15'],
        ['2020-03-20', '2020-04-20', '2020-05-20'],
        ['2020-04-08', '2020-04-15', '2020-04-22'],


    ]
    pay_pattern = pd.DataFrame(
        pay_method, columns=['second_max_date', 'max_date', 'date'])

    # Join first_sec_max and pay_pattern
    pred_result = first_sec_max.merge(
        pay_pattern, on=['max_date', 'second_max_date'], how='left')
    #pd.set_option('display.max_rows', None)
    pd.options.display.max_rows = 10
    pred_result['date'] = pred_result['date'].fillna('NA')
    pred_result_bankAcctID = pred_result[['bankAcctID', 'date']]
    # left join loginAcct & startBalanceInfo
    AcctInput = AcctInput.astype({"loginAcct": int})
    pred_result_custID = pred_result_bankAcctID.merge(
        AcctInput, left_on='bankAcctID', right_on='loginAcct', how='left')
    output_date = pred_result_custID[['custID', 'date']]
    output_date.columns = ['custID', 'date1']

################ Milestone 1 ######################
    from data_processor import DataProcessor
    import pandas as pd
    data_processer = DataProcessor("dd")
    data_processer.execute()
    people = data_processer.persons
    data = [[v.get_customer_id(), v.get_account_id(), v.fraud]
            for v in people.values()]
    #df = pd.DataFrame(data, columns = ['CustomerId', 'AccountId', 'verifiedId'])
    df = pd.DataFrame(data, columns=['custID', 'AccountId', 'verifiedId'])
    df['face'] = np.where((df['verifiedId'] == 0), 'NA', 0)
    output_face = df[['custID', 'face']]
    output_face['custID'] = output_face['custID'].astype(int)


################ Join Results ######################
    fraud_verify = output_fraud.merge(output_verify, on='custID', how='left')
    fraud_verify_date = fraud_verify.merge(
        output_date, on='custID', how='left')
    fraud_verify_date_face = fraud_verify_date.merge(
        output_face, on='custID', how='left')

    output_prep = fraud_verify_date_face.copy()
    output_prep['date'] = output_prep['date1']
    output_prep['date'] = np.where((output_prep['fraudster'] == 'NA') | (output_prep['rightAcctFlag'] == 'NA') | (
        output_prep['date1'] == 'NA') | (output_prep['face'] == 'NA'), 'NA', output_prep['date1'])

################ output csv ######################
    # output csv
    output = output_prep[['custID', 'date']]
    output.columns = ['loginID', 'date']

    @st.cache
    def convert_df_to_csv(df):
        # IMPORTANT: Cache the conversion to prevent computation on every rerun
        return df.to_csv(index=False).encode('utf-8')


else:
    st.text('Please enter url above and click the Submit button')
    st.text('Please allow some time for downloadable solution file to pop up')


if urlinput != '':
    st.title('Step 2: Download Result')
    # st.text('Step 2: Download Fraudster Detection Result')
    st.download_button(
        label='Download',
        data=convert_df_to_csv(output),
        file_name=csvname+'.csv',
        mime='text/csv',)
    st.text('Now download the csv file')
else:
    st.text('')
