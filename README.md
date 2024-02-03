# Remapping ElasticSearch

During my internship at Axpo Genova I faced the challenge to change mappings for indexes "for indexes on which data was being written. I soon noticed that ElasticSearch didn't offer a primitive to do so. 
In fact ElasticSearch just provides the reindex primitive to move all the data from one index to another (allowing mapping changes). As I had to make many "remapping" during my project I decided to script this solution in Python to make this remapping process happen as if it was happening "in place", without losing data during the process thanks to aliases.

More details in my [thesis](tesi_capiaghiL.pdf) (in Italian).

I plan to restructure the code and write a better documentation...

 

