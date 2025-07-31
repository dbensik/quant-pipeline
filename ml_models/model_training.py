import joblib
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.model_selection import train_test_split


class ModelTrainer:
    def __init__(self, data, features, target, test_size=0.2, random_state=42):
        """
        Initialize the ModelTrainer with data, features, and target.
        """
        self.data = data
        self.features = features
        self.target = target
        self.test_size = test_size
        self.random_state = random_state
        self.model = None
        self.X_train = None
        self.X_test = None
        self.y_train = None
        self.y_test = None
        self.predictions = None

    def split_data(self):
        """
        Split the data into training and testing sets.
        """
        X = self.data[self.features]
        y = self.data[self.target]
        self.X_train, self.X_test, self.y_train, self.y_test = train_test_split(
            X, y, test_size=self.test_size, random_state=self.random_state
        )
        return self.X_train, self.X_test, self.y_train, self.y_test

    def train_linear_model(self):
        """
        Train a linear regression model on the training data.
        """
        if self.X_train is None or self.y_train is None:
            self.split_data()
        self.model = LinearRegression()
        self.model.fit(self.X_train, self.y_train)
        return self.model

    def evaluate_model(self):
        """
        Evaluate the trained model using MSE and R².
        """
        if self.model is None:
            raise ValueError("Model not trained. Call train_linear_model() first.")
        self.predictions = self.model.predict(self.X_test)
        mse = mean_squared_error(self.y_test, self.predictions)
        r2 = r2_score(self.y_test, self.predictions)
        return mse, r2

    def save_model(self, filepath):
        """
        Save the trained model to a file.
        """
        if self.model is None:
            raise ValueError("No model to save. Train a model first.")
        joblib.dump(self.model, filepath)

    def load_model(self, filepath):
        """
        Load a model from a file.
        """
        self.model = joblib.load(filepath)
        return self.model
