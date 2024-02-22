import json  
import boto3                #   Amazon SDK
import pandas as pd         #   Formatting of excel file and performing operation on excel
import io                   #   Handling Input/Output
from io import BytesIO 
import botocore             #   ErrorHandling
import xlsxwriter           #   Writing to Excel
import awswrangler          #   Importing Openpyxl and performing complex read operations on it 
import openpyxl 
from datetime import datetime, timedelta, date  
from multipledispatch import dispatch    
     
key = 'Account Details for S3 Storage Information.xlsx'                 # ^^^^INPUT FOLDER NAME/INPUT DOCUMENT NAME
bucket = 'testbuckettest'                                              # ^^^^BUCKET NAME

s3=boto3.client('s3')
file_object =s3.get_object(Bucket=bucket, Key=key)
file_content = file_object['Body'].read()
b_file_content = io.BytesIO(file_content)                                
df= pd.read_excel(b_file_content)                                        # df = dataframe for pandas
df_sheet_index = pd.read_excel(b_file_content, sheet_name=0)             # first sheet from excel is read
h_column_list_of_excel_file = df_sheet_index.columns.ravel().tolist()
b_file_content.close()

acc_id=[]
acc_name=[]
account_id = []
name_missing_list = []
Comments = []
Reason_for_error = []
account_ID = []
Flag_for_name = False
Flag_for_bucket_permission_role_error = True 
acc_id_causing_error = []
acc_name_causing_error =[]
serial_number_for_comments_sheet = []
serial_number_for_comments = 0
Flag_for_id = False  
id_missing_list = [] 
accId=[]
accName = []  
accid_from_excel=df_sheet_index[h_column_list_of_excel_file[1]].tolist()
accName_from_excel=df_sheet_index[h_column_list_of_excel_file[2]].tolist() 
print(accid_from_excel) 
for i in range(len(accid_from_excel)):                            # this particular section reads account id and account name from the excel sheet
    if pd.isnull(accid_from_excel[i]) == False :    
        accId.append(int(accid_from_excel[i])) 
        accName.append(accName_from_excel[i])
    else: 
        id_missing_list.append(i+1)
        Flag_for_id = True 
        Reason_for_error.append("Account Id Missing") 
        Comments.append("Account Id Missing at {}".format(i+1))
        acc_name_causing_error.append(accName_from_excel[i]) 
        acc_id_causing_error.append("")
        serial_number_for_comments = serial_number_for_comments + 1 
        serial_number_for_comments_sheet.append(serial_number_for_comments) 
    
print(accId)

for each in range(len(accName)):                      # checks if the account name is missing in excel sheet
    if pd.isnull(accName[each])== False :   
        account_ID.append(accId[each])
        acc_name.append(accName[each]) 
    else:
        name_missing_list.append(i+1)
        Flag_for_name = True 
        Reason_for_error.append("Account Name Missing") 
        Comments.append("Account Name Missing at {}".format(each+1))
        acc_name_causing_error.append("")  
        acc_id_causing_error.append(accId[each])  
        serial_number_for_comments = serial_number_for_comments + 1 
        serial_number_for_comments_sheet.append(serial_number_for_comments) 
print(account_ID)         
for each in account_ID:
    account_id.append(str(each))
print(account_id) 

client = boto3.client('sts')            
master_acc_id = client.get_caller_identity()['Account']
print(master_acc_id) 

for each in account_id:
    if len(each)==12:
        acc_id.append(each)
    else :
        N=12-len(each)
        each = each.rjust(N + len(each), '0')
        acc_id.append(each)  
  
rolearn = []  
for each in range(len(acc_id)):                 # creates rolearn for cross account action
    if acc_id[each] != master_acc_id:
        rolearn.append("arn:aws:iam::{}:role/Cross_Account_Role".format(acc_id[each]))   # ^^^ROLE NAME
dict_for_name = dict(zip(acc_id,acc_name))        
print(rolearn)
Flag_for_role_error = False
Flag_for_bucket_permission_role_error = False

#--------------------Standard Report---------------------------------------------------------------------------

def storage_conversion(number_of_days,storage_class_required,bucket_name = None): 
    if bucket_name:
        Flag_for_bucket_entry = False   
        serial_number_for_comments_new = serial_number_for_comments
        serial_number = 0
        serial_number_stored_in_xlsx = [] 
        acc_id_stored_in_xlsx = []
        acc_name_stored_in_xlsx = []  
        bucket_stored_in_xlsx = []
        object_stored_in_xlsx = []
        storage_class_stored_in_xlsx = []
        
          
        for each in range(len(rolearn)):                   # This section of code performs the storage conversion for all the cross accounts
            try:
                sts_connection = boto3.client('sts')       # temporary credentials are created using sts
                acct_b = sts_connection.assume_role(
                RoleArn=rolearn[each],     
                RoleSessionName="Cross_Account_Role"                               # ^^^^ROLE NAME
                )    
                
                ACCESS_KEY = acct_b['Credentials']['AccessKeyId']
                SECRET_KEY = acct_b['Credentials']['SecretAccessKey']    
                SESSION_TOKEN = acct_b['Credentials']['SessionToken']
        
                s3_resource = boto3.resource('s3',                      
                aws_access_key_id=ACCESS_KEY,
                aws_secret_access_key=SECRET_KEY,
                aws_session_token=SESSION_TOKEN,
                    )
                client = boto3.client('s3',
                aws_access_key_id=ACCESS_KEY,
                aws_secret_access_key=SECRET_KEY,
                aws_session_token=SESSION_TOKEN,
                    )
                try:
                    
                    ACC_ID = rolearn[each].split(":")[4]
                    x =  bucket_name 
                    bucket = s3_resource.Bucket(x)
                    for object in bucket.objects.all():
                        
                        today = date.today()                              #today's date
                        today = datetime.strptime(str(today), "%Y-%m-%d") 
                        end = today - timedelta(days=number_of_days)    #  This gives today date - the number of days mentioned in the function
                        
                        last_modified_date = str(object.last_modified).split(" ")                 
                        last_modified_date = datetime.strptime(str(last_modified_date[0]), "%Y-%m-%d")    # this gives the last modified date for comparison
                        
                        
                        if end >= last_modified_date:   
                            print("yes")
                            copy_source = {
                            'Bucket': x,
                            'Key': object.key
                            }
                            
                            client.copy(
                              copy_source, x, object.key,   
                              ExtraArgs = {
                                'StorageClass': storage_class_required,    
                                'MetadataDirective': 'COPY'
                              }
                            )
                            object_stored_in_xlsx.append(object.key)                
                            serial_number = serial_number+1
                            serial_number_stored_in_xlsx.append(serial_number)
                            acc_id_stored_in_xlsx.append(ACC_ID)
                            bucket_stored_in_xlsx.append(x)
                            for ac_id,name in dict_for_name.items(): 
                                    if ac_id == ACC_ID: 
                                        acc_name_stored_in_xlsx.append(name)
                                        
                            response = client.get_object(Bucket=x,Key=object.key) 
                            if 'StorageClass' in response.keys():
                                storage_class_stored_in_xlsx.append(response['StorageClass'])
                            else :
                                storage_class_stored_in_xlsx.append(object.storage_class) 
                except botocore.exceptions.ClientError as error:
                    Flag_for_bucket_entry = True
                    break
                        
            except botocore.exceptions.ClientError as error:
                    Flag_for_bucket_permission_role_error = True
                    Comments.append(error)
                    serial_number_for_comments_new = serial_number_for_comments_new + 1
                    serial_number_for_comments_sheet.append(serial_number_for_comments_new)  
                    Reason_for_error.append("Assume Role Related")
                    ACC_ID = rolearn[each].split(":")[4] 
                    acc_id_causing_error.append(ACC_ID)
                    for ac_id,name in dict_for_name.items(): 
                        if ac_id == ACC_ID: 
                            acc_name_causing_error.append(name)  
                            
        for i in range(len(acc_id)):          # This section of code performs the storage conversion for all the master account
            if acc_id[i]==master_acc_id:
                s3_resource = boto3.resource('s3') 
                client = boto3.client('s3') 
                
                # print(bucket.name)
                try:
                    x = bucket_name 
                    bucket = s3_resource.Bucket(x)
                    for object in bucket.objects.all():
                        
                        today = date.today() 
                        today = datetime.strptime(str(today), "%Y-%m-%d") 
                        end = today - timedelta(days=number_of_days) # date - days 
                        
                        last_modified_date = str(object.last_modified).split(" ")                 
                        last_modified_date = datetime.strptime(str(last_modified_date[0]), "%Y-%m-%d") 
                        
                        print(last_modified_date, end)  
                        
                        if end >= last_modified_date:    
                            copy_source = {           
                            'Bucket': x,
                            'Key': object.key
                            }
                            
                            print(object.key)
                            # print(object.storage_class)  
                            client.copy(
                              copy_source, x, object.key,       #provides the different storage clsss
                              ExtraArgs = {
                                'StorageClass': storage_class_required,    
                                'MetadataDirective': 'COPY' 
                              }   
                            ) 
                            object_stored_in_xlsx.append(object.key)                
                            serial_number = serial_number+1
                            serial_number_stored_in_xlsx.append(serial_number)
                            acc_id_stored_in_xlsx.append(acc_id[i])
                            bucket_stored_in_xlsx.append(x)
                            acc_name_stored_in_xlsx.append(acc_name[i]) 
                            response = client.get_object(Bucket=x,Key=object.key) 
                            if 'StorageClass' in response.keys():
                                storage_class_stored_in_xlsx.append(response['StorageClass'])
                                print(response['StorageClass']) 
                            else :
                                storage_class_stored_in_xlsx.append(object.storage_class)  
                            
                            # response = object.get(
                            # IfMatch=xx) 
                            # print(response) 
                except botocore.exceptions.ClientError as error:
                    Flag_for_bucket_entry = True
                    print('yes')
                    print(Flag_for_bucket_entry) 
                    break    
                    
        data={'S No ':serial_number_stored_in_xlsx, 'Account Id':acc_id_stored_in_xlsx, 'Account Name':acc_name_stored_in_xlsx,'Bucket Name': bucket_stored_in_xlsx,'Object Name':object_stored_in_xlsx, 'Storage Class' : storage_class_stored_in_xlsx}
        data_frame=pd.DataFrame(data)
        
        data_for_error={'S.No':serial_number_for_comments_sheet, 'Account Id':acc_id_causing_error,'Account Name':acc_name_causing_error,'Possible Cause ':Reason_for_error, 'Comments':Comments}
        data_frame_error=pd.DataFrame(data_for_error)
        
        io_buffer = io.BytesIO()             # saving the excel in s3
        s3 = boto3.resource('s3')  
        writer = pd.ExcelWriter(io_buffer, engine='xlsxwriter')
        sheets_in_writer=['Storage Status','Comments']
        data_frame_for_writer=[data_frame, data_frame_error]
        for i,j in zip(data_frame_for_writer,sheets_in_writer):
            i.to_excel(writer,j,index=False)    
        workbook=writer.book
        header_format = workbook.add_format({'bold': True,'text_wrap': True,'size':12, 'font_color':'black','valign': 'center','fg_color':'FBB1A1','border': 1})
        max_col=4   
        header_format_comments = workbook.add_format({'bold': True,'text_wrap': True,'size':12, 'font_color':'black','valign': 'center','fg_color':'F2FBA1','border': 1}) 
        
        
        worksheet=writer.sheets["Storage Status"]        
        
        for col_num, value in enumerate(data_frame.columns.values): 
            worksheet.write(0, col_num, value, header_format) 
            worksheet.set_column(1, 4, 20)
            worksheet.set_column(3,3,50)
            worksheet.set_column(4,4,30) 
            worksheet.set_column(5,5,30) 
            
        worksheet=writer.sheets["Comments"]  
        
        for col_num, value in enumerate(data_frame_error.columns.values): 
            worksheet.write(0, col_num, value, header_format_comments)  
            worksheet.set_column(0,2,15)  
            worksheet.set_column(3,3,25)  
            worksheet.set_column(4,4,45)   
            
        filepath = 'Storage class - Bucket.xlsx'                            #^^^ ......name of the outputexcel sheet
        writer.save()     
        data = io_buffer.getvalue() 
        s3.Bucket('testbuckettest').put_object(Key=filepath, Body=data)  #^^^ Bucket Name
        io_buffer.close()   
        storage_conversion.has_been_called = True 
        return Flag_for_bucket_entry 
        
    else:
          
        serial_number_for_comments_new = serial_number_for_comments
        serial_number = 0
        serial_number_stored_in_xlsx = [] 
        acc_id_stored_in_xlsx = []
        acc_name_stored_in_xlsx = []  
        bucket_stored_in_xlsx = []
        object_stored_in_xlsx = []
        storage_class_stored_in_xlsx = []
        Flag_for_bucket_entry = False
          
        for each in range(len(rolearn)):                   # This section of code performs the storage conversion for all the cross accounts
            try:
                sts_connection = boto3.client('sts')       # temporary credentials are created using sts
                acct_b = sts_connection.assume_role(
                RoleArn=rolearn[each],     
                RoleSessionName="Cross_Account_Role"                               # ^^^^ROLE NAME
                )    
                
                ACCESS_KEY = acct_b['Credentials']['AccessKeyId']
                SECRET_KEY = acct_b['Credentials']['SecretAccessKey']    
                SESSION_TOKEN = acct_b['Credentials']['SessionToken']
        
                s3_resource = boto3.resource('s3',                      
                aws_access_key_id=ACCESS_KEY,
                aws_secret_access_key=SECRET_KEY,
                aws_session_token=SESSION_TOKEN,
                    )
                client = boto3.client('s3',
                aws_access_key_id=ACCESS_KEY,
                aws_secret_access_key=SECRET_KEY,
                aws_session_token=SESSION_TOKEN,
                    )
                    
                ACC_ID = rolearn[each].split(":")[4]
                
                for bucket in s3_resource.buckets.all():
                    print(bucket.name)
                    x = bucket.name   
                    bucket = s3_resource.Bucket(x)
                    for object in bucket.objects.all():
                        
                        today = date.today()                              #today's date
                        today = datetime.strptime(str(today), "%Y-%m-%d") 
                        end = today - timedelta(days=number_of_days)    #  This gives today date - the number of days mentioned in the function
                        
                        last_modified_date = str(object.last_modified).split(" ")                 
                        last_modified_date = datetime.strptime(str(last_modified_date[0]), "%Y-%m-%d")    # this gives the last modified date for comparison
                        
                        
                        if end >= last_modified_date:   
                            print("yes")
                            copy_source = {
                            'Bucket': x,
                            'Key': object.key
                            }
                            
                            client.copy(
                              copy_source, x, object.key,   
                              ExtraArgs = {
                                'StorageClass': storage_class_required,    
                                'MetadataDirective': 'COPY'
                              }
                            )
                            object_stored_in_xlsx.append(object.key)                
                            serial_number = serial_number+1
                            serial_number_stored_in_xlsx.append(serial_number)
                            acc_id_stored_in_xlsx.append(ACC_ID)
                            bucket_stored_in_xlsx.append(x)
                            for ac_id,name in dict_for_name.items(): 
                                    if ac_id == ACC_ID: 
                                        acc_name_stored_in_xlsx.append(name)
                                        
                            response = client.get_object(Bucket=x,Key=object.key) 
                            if 'StorageClass' in response.keys():
                                storage_class_stored_in_xlsx.append(response['StorageClass'])
                            else :
                                storage_class_stored_in_xlsx.append(object.storage_class) 
                        
                        
            except botocore.exceptions.ClientError as error:
                    Flag_for_bucket_permission_role_error = True
                    Comments.append(error)
                    serial_number_for_comments_new = serial_number_for_comments_new + 1
                    serial_number_for_comments_sheet.append(serial_number_for_comments_new)  
                    Reason_for_error.append("Assume Role Related")
                    ACC_ID = rolearn[each].split(":")[4] 
                    acc_id_causing_error.append(ACC_ID)
                    for ac_id,name in dict_for_name.items(): 
                        if ac_id == ACC_ID: 
                            acc_name_causing_error.append(name)  
                            
        for i in range(len(acc_id)):          # This section of code performs the storage conversion for all the master account
            if acc_id[i]==master_acc_id:
                s3_resource = boto3.resource('s3') 
                client = boto3.client('s3') 
                
                for bucket in s3_resource.buckets.all():
                    
                    # print(bucket.name)
                    x = bucket.name
                    bucket = s3_resource.Bucket(x)
                    for object in bucket.objects.all():
                        
                        today = date.today() 
                        today = datetime.strptime(str(today), "%Y-%m-%d") 
                        end = today - timedelta(days=number_of_days) # date - days 
                        
                        last_modified_date = str(object.last_modified).split(" ")                 
                        last_modified_date = datetime.strptime(str(last_modified_date[0]), "%Y-%m-%d") 
                        
                        print(last_modified_date, end)  
                        
                        if end >= last_modified_date:    
                            copy_source = {           
                            'Bucket': x,
                            'Key': object.key
                            }
                            
                            print(object.key)
                            # print(object.storage_class)  
                            client.copy(
                              copy_source, x, object.key,       #provides the different storage clsss
                              ExtraArgs = {
                                'StorageClass': storage_class_required,    
                                'MetadataDirective': 'COPY' 
                              }   
                            ) 
                            object_stored_in_xlsx.append(object.key)                
                            serial_number = serial_number+1
                            serial_number_stored_in_xlsx.append(serial_number)
                            acc_id_stored_in_xlsx.append(acc_id[i])
                            bucket_stored_in_xlsx.append(x)
                            acc_name_stored_in_xlsx.append(acc_name[i]) 
                            response = client.get_object(Bucket=x,Key=object.key) 
                            if 'StorageClass' in response.keys():
                                storage_class_stored_in_xlsx.append(response['StorageClass'])
                                print(response['StorageClass']) 
                            else :
                                storage_class_stored_in_xlsx.append(object.storage_class)  
                            
                            # response = object.get(
                            # IfMatch=xx) 
                            # print(response) 
                    bucket = s3_resource.Bucket(x)
                    
        data={'S No ':serial_number_stored_in_xlsx, 'Account Id':acc_id_stored_in_xlsx, 'Account Name':acc_name_stored_in_xlsx,'Bucket Name': bucket_stored_in_xlsx,'Object Name':object_stored_in_xlsx, 'Storage Class' : storage_class_stored_in_xlsx}
        data_frame=pd.DataFrame(data)
        
        data_for_error={'S.No':serial_number_for_comments_sheet, 'Account Id':acc_id_causing_error,'Account Name':acc_name_causing_error,'Possible Cause ':Reason_for_error, 'Comments':Comments}
        data_frame_error=pd.DataFrame(data_for_error)
        
        io_buffer = io.BytesIO()             # saving the excel in s3
        s3 = boto3.resource('s3')  
        writer = pd.ExcelWriter(io_buffer, engine='xlsxwriter')
        sheets_in_writer=['Storage Status','Comments']
        data_frame_for_writer=[data_frame, data_frame_error]
        for i,j in zip(data_frame_for_writer,sheets_in_writer):
            i.to_excel(writer,j,index=False)    
        workbook=writer.book
        header_format = workbook.add_format({'bold': True,'text_wrap': True,'size':12, 'font_color':'black','valign': 'center','fg_color':'FBB1A1','border': 1})
        max_col=4   
        header_format_comments = workbook.add_format({'bold': True,'text_wrap': True,'size':12, 'font_color':'black','valign': 'center','fg_color':'F2FBA1','border': 1}) 
        
        
        worksheet=writer.sheets["Storage Status"]        
        
        for col_num, value in enumerate(data_frame.columns.values): 
            worksheet.write(0, col_num, value, header_format) 
            worksheet.set_column(1, 4, 20)
            worksheet.set_column(3,3,50)
            worksheet.set_column(4,4,30) 
            worksheet.set_column(5,5,30) 
            
        worksheet=writer.sheets["Comments"]  
        
        for col_num, value in enumerate(data_frame_error.columns.values): 
            worksheet.write(0, col_num, value, header_format_comments)  
            worksheet.set_column(0,2,15)  
            worksheet.set_column(3,3,25)  
            worksheet.set_column(4,4,45)   
            
        filepath = 'Storage class - Account.xlsx'                             #^^^ ......name of the outputexcel sheet
        writer.save()     
        data = io_buffer.getvalue() 
        s3.Bucket('testbuckettest').put_object(Key=filepath, Body=data)  #^^^ Bucket Name
        io_buffer.close()   
        storage_conversion.has_been_called = True
        return Flag_for_bucket_entry

def lambda_handler(event, context):
     
    storage_conversion.has_been_called = False
    
    #  Please provide the following arguments to storage_conversion function 
    #        a)number of days
    #        b)the storage class as chosen from 'STANDARD','STANDARD_IA','GLACIER','ONEZONE_IA','INTELLIGENT_TIERING','DEEP_ARCHIVE'
    #        example : storage_conversion(0,'STANDARD_IA')
    #        c)bucket name 
    
    Flag = storage_conversion(0,'STANDARD','testaccess-01')             
    

    if storage_conversion.has_been_called == True and Flag == False:
        result = " Storage Conversion Performed "
        if Flag_for_name == True or Flag_for_role_error == True or Flag_for_bucket_permission_role_error == True:
            result += "....Some entries are missing. Please check the comments sheet" 
    elif storage_conversion.has_been_called == True and Flag == True:  
        result = " Wrong bucket entry "   
    return result
