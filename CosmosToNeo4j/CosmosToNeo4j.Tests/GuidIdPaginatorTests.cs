namespace CosmosToNeo4j.Tests;

using FluentAssertions;

public class GuidIdPaginatorTests
{
    public class GeneratePagingGremlinQueriesMethod
    {
        public IPaginator Paginator => new GuidIdPaginator();

        [Theory]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(18)]
        [InlineData(30)]
        [InlineData(35)]
        [InlineData(36)]
        public void GeneratesTheCorrectNumberOfPages(int numPages)
        {
            var pairings = Paginator.GeneratePagingPairs(numPages);
            pairings.Count().Should().Be(numPages);
        }

        [Fact]
        public void GeneratesAllV_WhenPagesEqualsOne()
        {
            var pairings = Paginator.GeneratePagingPairs(1).ToList();
            pairings.Count.Should().Be(1);
            pairings.First().ToGremlinNode().Should().Be("g.V()");
        }

        [Fact]
        public void GeneratesCorrectPairings_WhenPagesEqualsTwo()
        {
            var pairings = Paginator.GeneratePagingPairs(2).ToList();
            pairings.Count.Should().Be(2);
            
            var first = pairings.First();
            first.Start.Should().Be("0");
            first.End.Should().Be("i");

            var second = pairings.Last();
            second.Start.Should().Be("i");
            second.End.Should().BeNull();
        }

        [Fact]
        public void GeneratesCorrectPairings_WhenRequiringTwoCharacters()
        {
            var pairings = Paginator.GeneratePagingPairs(72).ToList();
            pairings.Count.Should().Be(72);
            var first = pairings.First();
            first.Start.Should().Be("0");
            first.End.Should().Be("0i");

            var second = pairings.Skip(1).First();
            second.Start.Should().Be("0i");
            second.End.Should().Be("10");

            var last = pairings.Last();
            last.Start.Should().Be("zi");
        }
    }
}