# Local Database Management System

# Project Summary
A self-contained relational DBMS built from scratch with a custom SQL engine, bit-level storage, and B+Tree indexing.

# Key Features
- Custom SQL-like DDL and DML parser
- Command-line interface for database interaction
- Bit-level record serialization and disk I/O
- Local storage manager with explicit data layout control
- B+Tree indexing for high-performance queries
- No external database dependencies

# Project Details
- This project is a self-contained relational database management system implemented from scratch, designed to explore the internal architecture of modern databases. The system includes a custom SQL-like query engine, a low-level storage manager, and an indexing subsystem, all without relying on existing database libraries.
- The database supports creating, storing, and querying local relational databases through a command-line interface. Queries are parsed using a custom-built DDL/DML parser and executed directly against on-disk data structures managed by the engine.
- At the storage layer, the system uses bit-level data serialization to read and write records directly to disk. This approach provides fine-grained control over data layout, minimizes storage overhead, and improves I/O efficiency while keeping the system fully self-contained.
- To address performance, the database implements B+Tree indexing, enabling efficient conditional lookups and range queries. The addition of indexing resulted in over a 500% improvement in query performance compared to full table scans.
- This project emphasizes database internals, systems-level programming, and performance-oriented design, mirroring many of the core concepts used in production-grade DBMS implementations.

##### To run this program on the CS machines from this directory:
- Compile using 'javac -d out -cp src src/Main.java src/Classes/\*.java src/WhereTree/\*.java src/BPlusTree/\*.java'
- Run using 'java -cp out Main \<dblocation> \<pagesize> \<buffersize> \<indexing>'
- Alternatively, you can change directory into .\out after compilation and run using 'java Main \<dblocation> \<pagesize> \<buffersize> \<indexing>'

##### Known issues:
- If pasting multi-line commands, you will probably need to hit enter one more time at the end to execute the final command. This happens if you copy-pasted the commands without a new-line/return at the end.
- When manually typing in commands, the command will appear twice. This is because for every command pasted in or entered, it appears once from you typing it and again from the program itself printing that command. This helps with multi-command inputs for clarity, but when typing individual inputs in it displays them twice.
