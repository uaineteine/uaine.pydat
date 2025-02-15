from pandas import DataFrame
from typing import List

def clean_whitespace_in_df(df: DataFrame) -> DataFrame:
    df_cleaned = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
    return df_cleaned

def replace_between_tags(content: str, tag_name: str, new_lines: List[str], deleteTags=False) -> str:
    start_tag = f'<{tag_name}>'
    end_tag = f'</{tag_name}>'
    
    start_index = content.find(start_tag)
    end_index = content.find(end_tag, start_index)

    if start_index == -1 or end_index == -1:
        raise ValueError("Tags not found in the content")

    #delete tags themeselves by modifying the selection index
    if (deleteTags):
        start_index = start_index - len(start_tag)
        end_index = end_index - len(end_tag)

    new_content = content[:start_index + len(start_tag)] + '\n' + '\n'.join(new_lines) + '\n' + content[end_index:]

    return new_content

def break_into_lines(string: str) -> list[str]:
    """
    Breaks a string into a list of lines.

    Args:
        string (str): The input string to be broken into lines.

    Returns:
        list[str]: A list of lines from the input string.
    """
    return string.split('\n')


def add_prefix(string: str, prefix: str) -> str:
    return prefix + string

def add_suffix(string: str, suffix: str) -> str:
    return string + suffix
