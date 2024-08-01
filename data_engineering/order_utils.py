from typing import Iterable

def create_desc_filter(columns_to_order_by:Iterable[str]) -> str:
    columns_to_order_by_with_order = [f'{column_to_order_by} DESC' for column_to_order_by in columns_to_order_by]
    return ', '.join(columns_to_order_by_with_order)