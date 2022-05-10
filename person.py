class Person:
    
    def __init__(self, customer_id, account_id):
        self.customer_id = customer_id
        self.account_id = account_id
        self.fraud = False


    def get_customer_id(self):
        return self.customer_id
    
    def get_account_id(self):
        return self.account_id
    
    def get_fraud(self):
        return self.fraud;

    def __repr__(self):
        return "CustomerId: " + self.customer_id + " AccountId: " + self.account_id + " isFraud: " + str(self.fraud);
        
    def set_fraud(self, num): 
        self.fraud = num