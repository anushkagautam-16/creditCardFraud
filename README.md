# creditCardFraud
about credit card frauds
from lxml import etree
import pandas as pd
import pyodbc

def extract_data_with_common_fields(xml_file, xpath_expr):
    # Step 1: Clean XML
    with open(xml_file, 'rb') as f:
        content = f.read()

    start = content.find(b'<?xml')
    cleaned = content[start:] if start != -1 else content

    tree = etree.fromstring(cleaned)

    # Namespaces
    ns = {
        'camt': 'urn:iso:std:iso:20022:tech:xsd:camt.053.001.02',
        'default': 'urn:iso:std:iso:20022:tech:xsd:camt.053.001.02'
    }

    # Step 2: Extract transaction-level data using XPath
    elements = tree.xpath(xpath_expr, namespaces=ns)
    if not elements:
        print("[WARNING] No matching elements found.")
        return None

    data = []
    for elem in elements:
        row = {}
        for child in elem.iter():
            tag = child.tag.split('}')[-1]
            if child.text and child.text.strip():
                row[tag] = child.text.strip()
        if row:
            data.append(row)

    df = pd.DataFrame(data)

    # Step 3: Extract common fields once
    def get(path):
        el = tree.find(path, namespaces=ns)
        return el.text.strip() if el is not None and el.text else ''

    common_fields = {
        'MsgId': get('.//camt:MsgId'),
        'CreDtTm': get('.//camt:CreDtTm'),
        'StmtId': get('.//camt:Stmt/camt:Id'),
        'PgNb': get('.//camt:PgNb'),
        'LastPgInd': get('.//camt:LastPgInd'),
        'LglSeqNb': get('.//camt:LglSeqNb'),
        'AcctId': get('.//camt:Acct/camt:Id/camt:Othr/camt:Id')
    }

    # Step 4: Add common fields to each row
    for key, value in common_fields.items():
        df[key] = value

    print("[INFO] Data extraction complete.")
    return df


# === Usage Example ===
xml_file = 'yourfile.xml'
xpath_expr = './/default:Ntry/default:NtryDtls/default:TxDtls/default:Refs'  # Update if needed

df = extract_data_with_common_fields(xml_file, xpath_expr)
print(df)

# Optional: call `save_to_db(df, 'YourTableName')` if you want to upload it to SQL Server
