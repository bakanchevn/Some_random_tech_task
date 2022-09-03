from csv import reader
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


def get_active_users(file_dir: str, file_name: str) -> List[str]:
    active_users= []
    # Open the file in read mode
    with open(file_dir + file_name, 'r') as read_obj:
    # pass the file object to reader() to get the reader object
        csv_reader = reader(read_obj)
    # Iterate over each row in the csv using reader object and get the row if the user is active
        for row in csv_reader:
            if row[1] == 'True':
                active_users.append(row[0])
    return active_users


def get_transactions_data(active_users_list: List[str], file_dir: str, file_name: str) -> List[List[str]]:
    transaction_categories = {}
    with open(file_dir + file_name, 'r') as read_obj:
        csv_reader = reader(read_obj)
        for row in csv_reader:
            # transactions_id - 0, date - 1, user_id - 2, is_blocked - 3, amount - 4, category - 5
            if row[3] != 'True':
                # getting rows only for active users
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



def get_category_info():
    active_users = get_active_users('source/', 'users.csv')
    transaction_categories = get_transactions_data(active_users, 'source/', 'transactions.csv')
    for cat in transaction_categories:
        print(cat[0], cat[1]['sum_amount'], cat[1]['users_cnt'])



if __name__ == '__main__':
    get_category_info()