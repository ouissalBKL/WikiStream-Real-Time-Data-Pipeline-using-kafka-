CREATE TABLE wiki_changes (
    ts DATETIME2,
    wiki NVARCHAR(255),
    username NVARCHAR(255),
    title NVARCHAR(255),
    type_change NVARCHAR(100),
    namespace_id INT,
    bot BIT
);
