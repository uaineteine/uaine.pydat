import numpy as np
import pandas as pd
import string
import random
from datetime import datetime, timedelta
from typing import List, Union, Callable, Optional, Tuple

def gen_string_column(
    size: int, 
    length: Union[int, Tuple[int, int]] = 10, 
    charset: str = None, 
    prefix: str = "", 
    suffix: str = "", 
    null_prob: float = 0.0,
    pattern: str = None
) -> pd.Series:
    """
    Generate a pandas Series of random strings.

    Parameters:
    -----------
    size : int
        Number of strings to generate
    length : int or tuple(int, int)
        If int, the exact length of each string
        If tuple, the (min, max) length range for random string length
    charset : str, optional
        String containing characters to use. If None, uses lowercase letters
    prefix : str, optional
        Prefix to add to each generated string
    suffix : str, optional
        Suffix to add to each generated string
    null_prob : float, optional
        Probability of generating a null value (0.0 to 1.0)
    pattern : str, optional
        Pattern to use for string generation with character classes:
        - 'L' = uppercase letter
        - 'l' = lowercase letter
        - 'd' = digit
        - 'c' = special character
        - 'a' = any alphanumeric character
        Example: 'Llldd-lldd' would generate something like 'Tgh45-jk78'

    Returns:
    --------
    pd.Series
        Series of randomly generated strings
    """
    if charset is None:
        charset = string.ascii_lowercase
    
    # Determine if we're using fixed or variable length
    variable_length = isinstance(length, tuple) and len(length) == 2
    
    # Generate strings according to pattern or random characters
    result = []
    for _ in range(size):
        # Decide if this should be a null value
        if random.random() < null_prob:
            result.append(None)
            continue
            
        if pattern:
            # Generate string based on the pattern
            s = ""
            for char in pattern:
                if char == 'L':
                    s += random.choice(string.ascii_uppercase)
                elif char == 'l':
                    s += random.choice(string.ascii_lowercase)
                elif char == 'd':
                    s += random.choice(string.digits)
                elif char == 'c':
                    s += random.choice(string.punctuation)
                elif char == 'a':
                    s += random.choice(string.ascii_letters + string.digits)
                else:
                    s += char  # Use the character as-is
        else:
            # Generate random string of specified length
            if variable_length:
                str_length = random.randint(length[0], length[1])
            else:
                str_length = length
                
            s = ''.join(random.choice(charset) for _ in range(str_length))
            
        # Add prefix and suffix
        s = prefix + s + suffix
        result.append(s)
        
    return pd.Series(result)

def gen_numeric_column(
    size: int, 
    data_type: str = 'float',
    min_val: Union[int, float] = 0, 
    max_val: Union[int, float] = 100, 
    distribution: str = 'uniform', 
    null_prob: float = 0.0,
    precision: int = None
) -> pd.Series:
    """
    Generate a pandas Series of random numbers.

    Parameters:
    -----------
    size : int
        Number of values to generate
    data_type : str
        Type of numeric data: 'int', 'float', or 'decimal'
    min_val : int or float
        Minimum value (inclusive)
    max_val : int or float
        Maximum value (inclusive for ints, exclusive for floats)
    distribution : str
        Distribution to use for generating values:
        - 'uniform': Uniform distribution between min and max
        - 'normal': Normal distribution with mean=(min+max)/2 and std=(max-min)/6
        - 'exponential': Exponential distribution
        - 'lognormal': Log-normal distribution
    null_prob : float
        Probability of generating a null value (0.0 to 1.0)
    precision : int, optional
        For float/decimal, number of decimal places to round to

    Returns:
    --------
    pd.Series
        Series of randomly generated numeric values
    """
    # Create mask for null values
    mask = np.random.random(size) < null_prob
    
    # Generate values based on distribution
    if distribution == 'uniform':
        if data_type == 'int':
            values = np.random.randint(min_val, max_val + 1, size=size)
        else:
            values = np.random.uniform(min_val, max_val, size=size)
    
    elif distribution == 'normal':
        mean = (min_val + max_val) / 2
        std = (max_val - min_val) / 6  # ~99.7% within min_val to max_val
        values = np.random.normal(mean, std, size=size)
        
        # Clip values to ensure they fall within the specified range
        values = np.clip(values, min_val, max_val)
        
        if data_type == 'int':
            values = np.round(values).astype(int)
    
    elif distribution == 'exponential':
        scale = (max_val - min_val) / 5  # Roughly align with range
        values = np.random.exponential(scale, size=size) + min_val
        
        # Clip values to ensure they fall within the specified range
        values = np.clip(values, min_val, max_val)
        
        if data_type == 'int':
            values = np.round(values).astype(int)
            
    elif distribution == 'lognormal':
        mean = np.log((min_val + max_val) / 2)
        sigma = 0.5  # Controls the shape of the distribution
        values = np.random.lognormal(mean, sigma, size=size)
        
        # Scale to the target range
        values = min_val + (values - min(values)) * \
                 (max_val - min_val) / (max(values) - min(values) or 1)
        
        if data_type == 'int':
            values = np.round(values).astype(int)
    
    else:
        raise ValueError(f"Unsupported distribution: {distribution}")
    
    # Apply precision if specified
    if precision is not None and data_type != 'int':
        values = np.round(values, precision)
    
    # Apply null mask
    result = pd.Series(values)
    result[mask] = None
    
    return result

def gen_date_column(
    size: int,
    start_date: Union[str, datetime] = '2020-01-01',
    end_date: Union[str, datetime] = '2023-12-31',
    date_format: str = '%Y-%m-%d',
    null_prob: float = 0.0,
    distribution: str = 'uniform'
) -> pd.Series:
    """
    Generate a pandas Series of random dates.

    Parameters:
    -----------
    size : int
        Number of dates to generate
    start_date : str or datetime
        Starting date (inclusive)
    end_date : str or datetime
        Ending date (inclusive)
    date_format : str
        Format string for date output (if returning strings)
    null_prob : float
        Probability of generating a null value (0.0 to 1.0)
    distribution : str
        Distribution to use for generating dates:
        - 'uniform': Uniform distribution between start and end dates
        - 'normal': Normal distribution centered on the midpoint
        - 'recent': Bias towards more recent dates

    Returns:
    --------
    pd.Series
        Series of randomly generated dates as strings in the specified format
    """
    # Convert string dates to datetime objects
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, date_format)
    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, date_format)
    
    # Calculate time range in days
    time_delta = (end_date - start_date).days
    
    # Create mask for null values
    mask = np.random.random(size) < null_prob
    
    # Generate random days based on distribution
    if distribution == 'uniform':
        days = np.random.randint(0, time_delta + 1, size=size)
    
    elif distribution == 'normal':
        mean = time_delta / 2
        std = time_delta / 6  # ~99.7% within the range
        days = np.random.normal(mean, std, size=size)
        days = np.clip(days, 0, time_delta).round().astype(int)
    
    elif distribution == 'recent':
        # Exponential distribution favoring recent dates
        days = np.random.exponential(time_delta / 5, size=size)
        days = (time_delta - np.clip(days, 0, time_delta)).round().astype(int)
    
    else:
        raise ValueError(f"Unsupported distribution: {distribution}")
    
    # Convert days to actual dates
    dates = [start_date + timedelta(days=int(day)) for day in days]
    
    # Format dates as strings
    formatted_dates = [date.strftime(date_format) for date in dates]
    
    # Create Series and apply null mask
    result = pd.Series(formatted_dates)
    result[mask] = None
    
    return result

def gen_categorical_column(
    size: int,
    categories: List[str] = None,
    weights: List[float] = None,
    null_prob: float = 0.0
) -> pd.Series:
    """
    Generate a pandas Series of random categorical values.

    Parameters:
    -----------
    size : int
        Number of values to generate
    categories : list of str
        List of possible categorical values
    weights : list of float, optional
        Probability weights for each category. Must sum to 1.0 if provided.
    null_prob : float
        Probability of generating a null value (0.0 to 1.0)

    Returns:
    --------
    pd.Series
        Series of randomly generated categorical values
    """
    if categories is None:
        categories = ['Category A', 'Category B', 'Category C']
    
    # Create mask for null values
    mask = np.random.random(size) < null_prob
    
    # Generate random categories
    result = pd.Series(np.random.choice(categories, size=size, p=weights))
    
    # Apply null mask
    result[mask] = None
    
    return result

def gen_bool_column(
    size: int,
    true_prob: float = 0.5,
    null_prob: float = 0.0,
    labels: Tuple[str, str] = None
) -> pd.Series:
    """
    Generate a pandas Series of random boolean values.

    Parameters:
    -----------
    size : int
        Number of values to generate
    true_prob : float
        Probability of generating a True value (0.0 to 1.0)
    null_prob : float
        Probability of generating a null value (0.0 to 1.0)
    labels : tuple(str, str), optional
        Custom labels for (False, True) values. If provided, returns strings instead of booleans.

    Returns:
    --------
    pd.Series
        Series of randomly generated boolean values or custom labels
    """
    # Create mask for null values
    mask = np.random.random(size) < null_prob
    
    # Generate random booleans
    values = np.random.random(size) < true_prob
    
    # Apply custom labels if provided
    if labels is not None:
        values = pd.Series([labels[1] if v else labels[0] for v in values])
    else:
        values = pd.Series(values)
    
    # Apply null mask
    values[mask] = None
    
    return values

def gen_dataframe(
    rows: int,
    columns: dict,
    include_id: bool = True
) -> pd.DataFrame:
    """
    Generate a pandas DataFrame with specified columns.

    Parameters:
    -----------
    rows : int
        Number of rows to generate
    columns : dict
        Dictionary where keys are column names and values are functions to generate the column data
    include_id : bool
        Whether to include an 'id' column with sequential integers

    Returns:
    --------
    pd.DataFrame
        Generated DataFrame with the specified columns
    """
    data = {}
    
    # Add ID column if requested
    if include_id:
        data['id'] = pd.Series(range(1, rows + 1))
    
    # Generate each column
    for col_name, generator_func in columns.items():
        data[col_name] = generator_func(rows)
    
    return pd.DataFrame(data)

def gen_sample_dataframe(rows: int, include_id: bool = True) -> pd.DataFrame:
    """
    Generate a sample pandas DataFrame with specified columns.
    """
    # Generate a sample dataframe
    sample_df = gen_dataframe(
        rows=rows,
        columns={
            'name': lambda n: gen_string_column(n, prefix="User_", pattern="Lllllll"),
            'age': lambda n: gen_numeric_column(n, data_type='int', min_val=18, max_val=90, null_prob=0.05),
            'salary': lambda n: gen_numeric_column(n, min_val=30000, max_val=150000, precision=2, distribution='lognormal'),
            'join_date': lambda n: gen_date_column(n, start_date='2015-01-01', distribution='recent'),
            'department': lambda n: gen_categorical_column(n, categories=['HR', 'Engineering', 'Sales', 'Marketing', 'Support']),
            'active': lambda n: gen_bool_column(n, true_prob=0.8, labels=('Inactive', 'Active'))
        },
        include_id=include_id
    )
    return sample_df

# Example usage (commented out)
# if __name__ == "__main__":
#     # Generate a sample dataframe
#     sample_df = gen_sample_dataframe(1000, include_id=True)
    
#     # Display the dataframe
#     print(sample_df.head(10))
    
#     # Show basic statistics
#     print("\nDataframe summary statistics:")
#     print(sample_df.describe(include='all'))
    
#     # Count null values
#     print("\nNull values per column:")
#     print(sample_df.isnull().sum())