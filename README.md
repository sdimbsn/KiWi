# KiWi

A Key-Value Map for Scalable Real-Time Analytics

KiWi is the first atomic KV-map to efficiently support simultaneous large scans and real-time access. The key to achieving this is treating scans as first class citizens, and organizing the data structure around them. KiWi provides wait-free scans, whereas its put operations are lightweight and lock-free. It optimizes memory management jointly with data structure access.

The full details of the algorithm and its evaluation can be found in KiWi: *A Key-Value Map for Scalable Real-Time Analytics*, by Dmitry Basin, Edward Bortnikov, Anastasia Braginsky, Guy Golan-Gueta, Eshcar Hillel, Idit Keidar and Moshe Sulamy, published in PPoPP 2017.
