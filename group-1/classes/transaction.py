class Transaction:
    def __init__(self, total, customer_name, date, location):
        self.total = total
        self.customer_name = customer_name
        self.date = date
        self.location = location
    def __repr__(self):
        return f'Customer name is {self.customer_name}.'

