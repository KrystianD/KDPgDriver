Examples
======

Models definitions:

```c#
[KDPgTable("author", schema: "public")]
public class Author
{
  [KDPgColumn("id", KDPgColumnFlagsEnum.PrimaryKey | KDPgColumnFlagsEnum.AutoIncrement)]
  public int Id { get; set; }

  [KDPgColumn("name")]
  public string Name { get; set; }

  [KDPgColumn("age")]
  public int Age { get; set; }
}

[KDPgTable("book", schema: "public")]
public class Book
{
  [KDPgColumn("id", KDPgColumnFlagsEnum.PrimaryKey | KDPgColumnFlagsEnum.AutoIncrement)]
  public int Id { get; set; }

  [KDPgColumn("name")]
  public string Name { get; set; }

  [KDPgColumn("author_id")]
  public int AuthorId { get; set; }

  [KDPgColumn("pages")]
  public int Pages { get; set; }
}
```

Driver initialization:

```c#
var driver = new Driver("postgresql://postgres:test@localhost:9999/postgres", "public");
await driver.InitializeAsync();
```

Simple query:

```c#
var results1 = await driver.From<Author>()
                           .Select()
                           .Where(x => x.Name.StartsWith("John"))
                           .ToListAsync();
```

Left Join query:
        
```c#                   
var results2 = await driver.FromMany<Author, Book>((user, book) => user.Id == book.AuthorId)
                           .Map((user, book) => new {
                               User = user,
                               Book = book,
                           })
                           .Select(x => new {
                               UserName = x.User.Name,
                               Book = x.Book,
                           })
                           .Where(x => x.User.Age > 20 && x.Book.Pages > 1000)
                           .ToListAsync();
```

Create transaction batch (all queries will be sent to the database in one go):

```c#
var tr = driver.CreateTransactionBatch();
```

Simple insert:

```c#
tr.Insert(new Book() {
     Name = "book-name",
     Pages = 123,
     AuthorId = 1,
  })
  .UseField(x => x.Name)
  .UseField(x => x.Pages)
  .UseField(x => x.AuthorId)
  .Schedule();
```

Simple update:

```c#
tr.Update<Book>()
  .SetField(x => x.Pages, 123)
  .Where(x => x.Id == 2)
  .Schedule();
```

Execute the transaction:

```c#
await tr.Execute();
```
