print("[test_extract] testing extract using soup")
import arxiv_paper

p = arxiv_paper.ArxivPaper("0704.0003")
p.extract_using_soup()
print(p)



print("[test_extract] testing extract using API")
import query
id_list = ["1802.06593", "0704.0003"]
repeated_list = id_list * 30
q = query.build_base_query_url(repeated_list)
q = query.page_query_url(q, 0)
xml = query.xml_query(q)
print(xml)
entries = query.parse_arxiv_xml(xml)
print("")
print(entries)

print("[test_extract] testing extract using API for ID that doesn't exist")
import query
q = query.build_base_query_url(["0704.9000"])
q = query.page_query_url(q, 0)
xml = query.xml_query(q)
print(xml)
entries = query.parse_arxiv_xml(xml)
print("")
print(entries)
