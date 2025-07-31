import pandas as pd
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler


class PrincipalComponentAnalyzer:
    """
    Performs Principal Component Analysis (PCA) on a DataFrame of asset returns.
    """

    def __init__(self, returns_df: pd.DataFrame, n_components: int = None):
        """
        Initializes the analyzer with asset returns.

        Args:
            returns_df: A DataFrame where each column is an asset and each row is a return.
            n_components: The number of principal components to compute. Defaults to all.
        """
        if returns_df.empty or returns_df.isna().all().all():
            raise ValueError("Input DataFrame for PCA cannot be empty or all NaNs.")

        self.returns_df = returns_df.dropna()
        self.scaler = StandardScaler()
        self.pca = PCA(n_components=n_components)
        self.results = {}

    def run(self) -> dict:
        """
        Executes the PCA workflow: scaling, fitting, and result extraction.

        Returns:
            A dictionary containing PCA results, including explained variance and components.
        """
        scaled_data = self.scaler.fit_transform(self.returns_df)
        self.pca.fit(scaled_data)

        self.results = {
            "explained_variance_ratio": self.pca.explained_variance_ratio_,
            "cumulative_explained_variance": self.pca.explained_variance_ratio_.cumsum(),
            "components": pd.DataFrame(
                self.pca.components_,
                columns=self.returns_df.columns,
                index=[f"PC_{i+1}" for i in range(self.pca.n_components_)],
            ),
            "eigenvalues": self.pca.explained_variance_,
        }
        return self.results