namespace Deploy
{
    using System;

    public class ApplicationMapping
    {
        public string Id { get; set; }
        public string SourceDirectory { get; set; }
        public Version Version { get; set; }
    }
}