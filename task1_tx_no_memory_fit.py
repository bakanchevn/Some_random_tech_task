import os
from csv import reader, writer
from decimal import Decimal
from typing import List

# Query to recreate:
# SELECT
#   t.transaction_category_id,
#   SUM(t.transaction_amount) AS sum_amount,
#   COUNT(DISTINCT t.user_id) AS num_users
# FROM transactions t
# JOIN users u USING (user_id)
# WHERE t.is_blocked = False
#   AND u.is_active = True
# GROUP BY t.transaction_category_id
# ORDER BY sum_amount DESC;

def get_active_users(file_dir: str, file_name: str, batch_size: int, offset: int) -> list[str] | None:
    active_users= []
    # Open the file in read mode
    with open(file_dir + file_name, 'r') as read_obj:
    # pass the file object to reader() to get the reader object
        csv_reader = reader(read_obj)
    # skip the offset number of rows in the file, if offset is more than the number of rows in the file, returns the empty list
        for t in range(offset):
            a = next(csv_reader, None)
            if not a:
                return None

    # Iterate over each row in the csv using reader object and get the row if the user is active
        for _ in range(batch_size):
            row = next(csv_reader, None)
            if not row:
                break
            if row[1] == 'True':
                active_users.append(row[0])
    return active_users

def get_transactions_data(active_users_list: List[str], file_dir: str, file_name: str) -> List[List[str]]:
    transaction_categories = {}
    with open(file_dir + file_name, 'r') as read_obj:
        csv_reader = reader(read_obj)
    # Iterate over each row in the csv using reader object and get the row if the user is active
        for row in csv_reader:
            # transactions_id - 0, date - 1, user_id - 2, is_blocked - 3, amount - 4, category - 5
            if row[3] != 'True':
                # getting rows only for current batch of active users
                if row[2] in active_users_list:
                    # if category is already in the dictionary, add the amount to the existing value
                    if row[5] in transaction_categories:
                        transaction_categories[row[5]]['sum_amount'] += Decimal(row[4])
                        # if user is not in the list of users for the category, add it
                        if row[2] not in transaction_categories[row[5]]['users']:
                            transaction_categories[row[5]]['users'][row[2]] = 1
                    else:
                        transaction_categories[row[5]] = {'sum_amount': Decimal(row[4]), 'users': {row[2]: 1}}
    # getting the number of users for each category
    transaction_categories_agg = {k:{'sum_amount': v['sum_amount'], 'users_cnt': len(v['users'])}  for k, v in transaction_categories.items()}
    # sorting the dictionary by the sum_amount
    transaction_categories_sorted = sorted(transaction_categories_agg.items(), key=lambda x: x[1]['sum_amount'], reverse=True)
    return transaction_categories_sorted

def create_temporary_folder(file_path: str) -> bool:
    try:
        os.makedirs(file_path + 'output/temp/', exist_ok=True)
        return True
    except OSError:
        print ("Creation of the directory %s failed" % file_path)
        return False

def drop_temporary_files(file_path: str) -> bool:
    try:
        for x in os.listdir(file_path + 'output/temp/'):
            os.remove(file_path + 'output/temp/' + x)
        return True
    except OSError:
        print ("Deletion of the files in the directory %s failed" % file_path)
        return False


def calculate_map_counts(temp_folder_name: str, batch_chunk_size: int = 10000) -> bool:
    create_temporary_folder(temp_folder_name)
    drop_temporary_files(temp_folder_name)
    i = 0
    offset = 0
    while (True):
        active_users = get_active_users('source/', 'users.csv', batch_chunk_size, offset)
        if not active_users:
            break
        tx_data = get_transactions_data(active_users, 'source/', 'transactions.csv')
        tx_list = [[k, v['sum_amount'], v['users_cnt']] for k, v in tx_data]
        with open('output/temp/stask1_tx_no_memory_fit_' + str(i) + '.csv', 'w+') as write_obj:
            csv_writer = writer(write_obj)
            csv_writer.writerows(tx_list)
        offset += batch_chunk_size
        i += 1
    return True

def reduce_counts(temp_folder_name: str) -> List[List[str]]:
    d = {}
    for x in os.listdir(temp_folder_name):
        with open(temp_folder_name + x, 'r') as read_obj:
            csv_reader = reader(read_obj)
            for row in csv_reader:
                if row[0] in d:
                    d[row[0]]['sum_amount'] += Decimal(row[1])
                    d[row[0]]['users_cnt'] += int(row[2])
                else:
                    d[row[0]] = {'sum_amount': Decimal(row[1]), 'users_cnt': int(row[2])}
    tx_data = sorted(d.items(), key=lambda x: x[1]['sum_amount'], reverse=True)
    return [[k, v['sum_amount'], v['users_cnt']] for k, v in tx_data]


def get_category_info():
    calculate_map_counts(os.getcwd() + '/')
    tx_data = reduce_counts(os.getcwd() + '/output/temp/')
    for cat in tx_data:
        print(cat[0], cat[1], cat[2])


if __name__ == '__main__':
    get_category_info()
