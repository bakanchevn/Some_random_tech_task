import csv 
from decimal import Decimal
import heapq as hq
from typing import List, Set

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


def get_active_users(file_path: str) -> Set[str]:
    active_users = set([])
    # Open the file in read mode
    with open(file_path, 'r') as csvfile:
        # pass the file object to reader() to get the reader object
        csv_reader = csv.DictReader(csvfile, fieldnames=['user_id', 'is_active'])
        # Iterate over each row in the csv using reader object and get the row if the user is active
        for row in csv_reader:
            user_id = row.get('user_id')
            if row.get('is_active') == 'True' and user_id:
                active_users.add(user_id)
    return active_users


def get_transactions_data(active_users: Set[str], file_path: str) -> List[List[str]]:
    transaction_categories = {}
    with open(file_path, 'r') as csvfile:
        csv_reader = csv.DictReader(csvfile, fieldnames=['transaction_id','date','user_id','is_blocked','transaction_amount','transaction_category_id'])
        for row in csv_reader:
            # transactions_id - 0, date - 1, user_id - 2, is_blocked - 3, amount - 4, category - 5
            if row.get('is_blocked') == 'False' and row.get('user_id') in active_users:
                category = row["transaction_category_id"]
                inner_dict = transaction_categories.setdefault(category, {'sum_amount': 0, 'users': set([])})
                inner_dict['sum_amount'] += Decimal(row['transaction_amount'])
                inner_dict['users'].add(row['user_id'])

    for v in transaction_categories.values():
        v['users'] = len(v['users'])

    
    return sorted(transaction_categories.items(), key=lambda x: x[1]['sum_amount'], reverse=True)



def get_category_info():
    for cat in get_transactions_data(get_active_users('./users.csv'), './transactions.csv'):
        print(cat[0], cat[1]['sum_amount'], cat[1]['users'])



if __name__ == '__main__':
    get_category_info()