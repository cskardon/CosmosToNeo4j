namespace CosmosToNeo4j;

public interface IPaginator
{
    IEnumerable<Pairing> GeneratePagingPairs(int numPages);
}

public class GuidIdPaginator : IPaginator
{
    private static readonly char[] IdChars = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z' };

    private static int NumberOfCharacters(int numPages)
    {
        for (int i = 1; i < 32; i++)
        {
            var amount = Math.Pow(IdChars.Length, i);
            if (numPages <= amount)
                return i;
        }

        throw new ArgumentOutOfRangeException(nameof(numPages), numPages, $"Unable to generate {numPages} pages, the maximum achievable is {IdChars.Length ^ 32}");
    }

    public IEnumerable<Pairing> GeneratePagingPairs(int numPages)
    {
        if(numPages == 1)
            return new []{new Pairing()};

        if (NumberOfCharacters(numPages) == 1)
            return PairingsForSingleCharacters(numPages);

        return PairingsForMultipleCharacters(numPages);
    }
    /*
     *g.V(v1).out().has(‘id’, gte(‘A’)).has(‘id’, lte(‘B’)).out()

g.V(v1).out().has(‘id’, gte(‘C’)).has(‘id’, lte(‘D’)).out()
     *
     */
    private static IEnumerable<Pairing> PairingsForMultipleCharacters(int numPages)
    {
        /*
         * General thoughts - #
         * Get number of repetitions
         * Create Pairings where the START = IdChars[i]->x number of repeats
         */

        var repetitions = numPages * 1.0 / IdChars.Length;
        if (repetitions % 1 != 0)
        {
            //Something
        }


        /*var jumpsPerPage = 36.0 / numPages;
       var pairings = new List<Pairing>();
       
       var prevI = 0;
       var jumpTotal = 0.0;
       var pageCounter = 0;
   
       while(pageCounter < numPages - 1){
           jumpTotal += jumpsPerPage;
           var nextI = prevI + 1;
           if (jumpTotal % 1 == 0 && Math.Abs(jumpsPerPage - 1) > double.Epsilon)
           {
               if(jumpsPerPage <= 2)
                   nextI++;
               else
                   nextI += (int)jumpsPerPage - 1;
           }

           if(nextI >= IdChars.Length){
               nextI--;
               if(nextI == prevI)
                   break;
           }
       
           if(nextI >= IdChars.Length)
               break;

           pairings.Add(new Pairing{Start = IdChars[prevI].ToString(), End = IdChars[nextI].ToString() });
           prevI = nextI;
           pageCounter++;
       }

       if (prevI < numPages - 1)
           prevI++;    

       pairings.Add(new Pairing { Start = IdChars[prevI].ToString()});
       return pairings;*/


        return Array.Empty<Pairing>();
    }

    private static IEnumerable<Pairing> PairingsForSingleCharacters(int numPages)
    {
        var pairings = new List<Pairing>();

        var jumpsPerPage = 36.0 / numPages;
        var prevI = 0;
        var jumpTotal = 0.0;
        var pageCounter = 0;
    
        while(pageCounter < numPages - 1){
            jumpTotal += jumpsPerPage;
            var nextI = prevI + 1;
            if (jumpTotal % 1 == 0 && Math.Abs(jumpsPerPage - 1) > double.Epsilon)
            {
                if(jumpsPerPage <= 2)
                    nextI++;
                else
                    nextI += (int)jumpsPerPage - 1;
            }

            if(nextI >= IdChars.Length){
                nextI--;
                if(nextI == prevI)
                    break;
            }
        
            if(nextI >= IdChars.Length)
                break;

            pairings.Add(new Pairing{Start = IdChars[prevI].ToString(), End = IdChars[nextI].ToString() });
            prevI = nextI;
            pageCounter++;
        }

        if (prevI < numPages - 1)
            prevI++;    

        pairings.Add(new Pairing { Start = IdChars[prevI].ToString()});
        return pairings;
    }

}