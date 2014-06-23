%% 032491821 
%% Lab 13 - Mapreduce 
%% To run, you have several options (this is for timing purposes mostly)
%% All runs will output two result files - results.txt and probabilities.txt.
%% When no file name specified - assumes that the split up files (a-z) already exist from previous runs.
%%
%% 1. Single node, no spawning with NO file-splitting (for benchmarking): 
%%		run lab13:onlyOne("filename.txt").
%%
%% 2. Single node/multinode with NO file splitting (for checking different timing issues):
%% 		run: lab13:main(). or lab13:main(['a@Computer','b@Computer',...]).
%%	This DOESN'T create onlya.txt through onlyz.txt but assumes they exist!
%%
%% 3. Single node/multinode with file name - meaning a file will now be split up. This is useful for the first time
%%		run: lab13:main([],"354984si.ngl"). or lab13:main(['a@Computer','b@Computer',...],"354984si.ngl").
%%	This also creates the onlya.txt through onlyz.txt
%%
%% Example test runs:
%% 1> timer:tc(readFile,onlyOne,["354984si.ngl"]).
%% {7379000,true}
%%
%% (a@Jengapad)1> timer:tc(readFile,main,[['a@Jengapad']]).
%% {6238000,ok}
%%
%% (a@Jengapad)2> timer:tc(readFile,main,[['a@Jengapad','b@Jengapad']]).
%% {5476000,ok}
%%
%% (a@Jengapad)3> timer:tc(readFile,main,[['a@Jengapad','b@Jengapad','c@Jengapad']]
%% {4715000,ok}
%%
%% (a@Jengapad)4> timer:tc(readFile,main,[['a@Jengapad','b@Jengapad','c@Jengapad','d@Jengapad']]).
%% {5434000,ok}
%%
%% (a@Jengapad)2> timer:tc(readFile,main,[['a@Jengapad','b@raspbmc','c@Jengapad','d@Jengapad']]).
%% {16023000,ok}

-module(lab13).
-compile(export_all).

%% Deal with only one node of the running. Will split up a file and compute everything
onlyOne(File) ->
    {ok,Data}=file:read_file(File), % Open file
    SplitBinary = binary:split(Data,[<<"\n">>], [global]), % Split read data into binaries
    ListOfWords = lists:map(fun(X)-> binary_to_list(X) end, SplitBinary), % Convert binary into list
    Words = tl(lists:reverse(ListOfWords)), % Chop off last word
    ets:new(a, [set, named_table]), % Create a new ETS for data logging
    lists:map(fun(X) ->
		      countCombos(a,X) % Count combinations on all words
	      end, Words),
    Dict = orddict:from_list(ets:tab2list(a)), % Put data into ordered dictionary
    {ok,Wd} = file:open("results.txt", [write]), 
    printDict(Dict,Wd), % Output combinations to file
    Totals = countTotal(Dict), % Count total number of appearances
    {ok,Wd2} = file:open("probabilities.txt", [write]),
    printDict(calcProb(Dict,Totals),Wd2), % And output the probabilities to a file aswell
    ets:delete(a).

%% Main entry point when no nodes specified. Will only run on a single erlang node. When no file is given, will use existing files already split.
main([]) ->
    main([node()]);

%% Main entry point when nodes ARE specified. Will randomly pick a node to start processes on. When no file is given, will use existing files already split.
main(Nodes) ->
    process_flag(trap_exit, true),
    mainWaitForFile(Nodes).

%% Main entry point when no nodes are specified but a file is given. Will split up the file first and then run on a single node.
main([],File)->
    main([node()],File);

%% Main entry point when nodes are specified and a file is given. Will split up the file first and then run randomly on several nodes.
main(Nodes,File) ->
    process_flag(trap_exit, true),
    ClearFile = fun(X) ->
			{ok,Wd} = file:open("only"++[X]++".txt", [write]), % Create a file for each letter a-z
			file:close(Wd)
		end,
    [ClearFile(X) || X<-lists:seq(97,122)],

    {ok,Data}=file:read_file(File), % Read data into binaries
    SplitBinary = binary:split(Data,[<<"\n">>], [global]), % Split binaries
    ListOfWords = lists:map(fun(X)-> binary_to_list(X) end, SplitBinary), % Turn binaries into a list
    readLines(tl(lists:reverse(ListOfWords)),Nodes). % Now start saving letters in their respective files.

%% Returning point from file-word splitter    
mainWaitForFile(Nodes) ->
    Letters = lists:seq(97,122),
    _Mappers = lists:map(fun(X) ->
				 spawn_link(pickRandomPid(Nodes), ?MODULE, mapper, [X,self()]) % Generate 26 mappers on random nodes
			 end, Letters),
    Reducers = lists:map(fun(X) -> 
				 spawn_link(pickRandomPid(Nodes), ?MODULE, reducer, [X,self(),0]) % Generate 26 reducers on random nodes
			 end , Letters),
    mainWaitForMappers(Reducers,length(Letters)). % Now wait for results from the mappers

%% Last event: Mappers are done
mainWaitForMappers(Pids,0) ->
    [Pid!finish || Pid <- Pids], % Send finish to all reducers, because we have finished sending them all data.
    Dict = mainWaitForResults(26,orddict:new()), % Wait for results from 26 reducers. After that,
    {ok,Wd} = file:open("results.txt", [write]),
    printDict(Dict,Wd), % Save results to a file
    Totals = countTotal(Dict),
    {ok,Wd2} = file:open("probabilities.txt", [write]),
    printDict(calcProb(Dict,Totals),Wd2); % And probabilities too

%% Not last event: Still waiting for more mappers
mainWaitForMappers(Pids,M) ->
    receive
	{intermediate,K} -> % Get interim results from the mapper
	    sendToReducer(Pids,K), % Send result to a specific reducer
	    mainWaitForMappers(Pids,M); % Recurse
	{'EXIT',_Who,_Why} ->
	    mainWaitForMappers(Pids,M-1) % Trapped an exit from a mapper, so we are sure that mapper has finished. We can reduce the number we're waiting for
    end.

%% Done waiting for results, return the dictionary as is.
mainWaitForResults(0,Dict) ->
    Dict;

%% Waiting for final results from the reducers
mainWaitForResults(M,Dict) ->
    receive
	{result,K} ->
	    Dict1=extractK(K,Dict), % Extract the results from the message into the dictionary
	    mainWaitForResults(M,Dict1);
	{'EXIT',_Who,_Why} ->
	    mainWaitForResults(M-1,Dict) % Recurse with M-1 because we trapped an exit and now need M-1 replies.
    end.

%% Folds over a dictionary, summing up the V values in {K,V}
countTotal(Dict) ->
    Sum = fun(_K,V,Acc) ->
		  tryhd(V)+Acc
	  end,
    orddict:fold(Sum,0,Dict).

%% Maps over a dictionary returning the probabilities, when given a sum from the previous method (countTotal)
calcProb(Dict,Sum) ->
    Prob=fun(_K,V) ->
		 tryhd(V)/Sum
	 end,
    orddict:map(Prob,Dict).

%% tries to hd() when it is a list. Otherwise just returns the value. Quick and dirty fix :S
tryhd(V) when is_list(V)==true ->
    hd(V);
tryhd(V) -> V.

%% Picks a random PID, when running on several nodes
pickRandomPid(Pids) ->
    case random:uniform(length(Pids)) of
	0 ->
	    pickRandomPid(Pids);
	N ->
	    lists:nth(N, Pids)
    end.

%% Will save words into their respective files
readLines(Data,Nodes) ->
    OpenFile = fun(X) -> % Function that opens a file and returns a write descriptor
		       {ok,Wd} = file:open("only"++[X]++".txt", [write]), %% Clear out files for writing
		       Wd
	       end,
    Wds = [OpenFile(X) || X<-lists:seq(97,122)], % Open 26 files
    readLines(Data,Nodes,Wds,0). % And start saving the words

%% Last event: Output a message saying we're done and close all write descriptors
readLines([],Nodes,Wds,N) ->
    io:format("Read ~w lines ~n",[N]), % Output message saying how many words were read
    [file:close(X) || X<-Wds], % Close write descriptors
    mainWaitForFile(Nodes); % Now start mapping

%% Not last event: Read more words and save in proper file
readLines([H|T],Nodes,Wds,N) ->
    First = firstLetter(H), % What is the first letter?
    if 
	First > 122-> % Not a letter a-z - ignore
	    readLines(T,Nodes,Wds,N);
	First < 97 -> % Not a letter a-z - ignore
	    readLines(T,Nodes,Wds,N);
	length(H) < 2 -> % Short word - ignore
	    readLines(T,Nodes,Wds,N);
	true -> 
	    io:fwrite(lists:nth(First-96,Wds),"~s~n",[H]), % Save word to file
	    readLines(T,Nodes,Wds,N+1) % Recurse
    end.

%% Pushes {K,V} from a list into a dictionary
extractK([],Dict) ->
    Dict;
extractK([H|T],Dict) ->
    {K,V}=H,
    Dict1=orddict:append(K,V,Dict),
    extractK(T,Dict1).

%% Prints a list to the screen nicely
printK([]) ->
    ok;
printK([H|T]) ->
    {K,V}=H,
    io:format("~s: ~w times~n",[K,V]),
    printK(T).

%% Saves a list to a file nicely
saveK([],Wd) ->
    file:close(Wd);

saveK([H|T],Wd) ->
    {K,V}=H,
    io:fwrite(Wd,"~s: ~c~c ~w\r\n",[K,9,9,V]),
    saveK(T,Wd).

%% Saves a dictionary to a file nicely (converts it into a list first)
printDict(Dict,Wd) ->
    saveK(orddict:to_list(Dict),Wd).

sendToReducer(_,[]) ->
    ok;

%% Sends a {K,V} from a list to a specific reducer based on the relevant letter, given a list of 26 reducers a-z.
sendToReducer(Reducers,[H|T]) ->
    {K,V} = H,
    case K of
	totalCount ->
	    sendToReducer(Reducers,T); % Ignore 'totalCount' entries
	_ ->
	    SendTo = lists:nth(hd(K)-96,Reducers), % Find specific reducer
	    SendTo ! {reduce,{K,V}}, % Send to that reducer
	    sendToReducer(Reducers,T) % Recurse
    end.

%% Reducer entry point
reducer(Letter,Parent,0) ->
    Tid = ets:new(list_to_atom("reduce"++[Letter]), [set, named_table]), % Create a new ETS table
    reducer(Tid,Parent,1);

reducer(Tid,Parent,1) ->
    EtsName = Tid,
    receive
	{reduce,{K,V}} ->
	    Res = ets:lookup(EtsName,K), % Lookup if we already have that entry
	    case length(Res) of
		0 ->
		    ets:insert(EtsName,{K,V}); % If not, insert
		_ ->
		    {_,C}=hd(Res),
		    ets:insert(EtsName,{K,C+V}) % Insert with altered counter if it already exists
	    end,
	    reducer(Tid,Parent,1)
    after 0 ->
	    receive
		finish -> Parent ! {result,ets:tab2list(EtsName)} % If no more things to reduce, and a finish message has been received - send data back
	    after 0 ->
		    reducer(Tid,Parent,1) % Otherwise keep waiting
	    end
    end.
    
firstLetter([H|_T]) ->
    H.

countCombos(_,Letters) when length(Letters)<2 ->
    ok; % No more letter combos, finish

%% Count combinations of two-letter blocks
countCombos(Letter,[H1,H2|Rest]) ->
    if
	H1 > 122-> % First letter not a letter a-z - remove this letter and keep going
	    countCombos(Letter,[H2|Rest]);
	H1 < 97 ->  % Fist letter not a letter a-z - remove this letter and keep going
	    countCombos(Letter,[H2|Rest]);
	H2 > 122->  % Second letter not a letter a-z - remove two letters and keep going
	    countCombos(Letter,Rest);
	H2 < 97 ->  % Second letter not a letter a-z - remove two letters and keep going
	    countCombos(Letter,Rest);
	true ->
	    Res=ets:lookup(Letter,[H1,H2]), % Check if we already have this entry
	    case length(Res) of
		0 ->
		    ets:insert(Letter,{[H1,H2],1}); % New entry - insert with counter 1
		1 ->
		    {_,C}=hd(Res),
		    ets:insert(Letter,{[H1,H2],C+1}) % Already exists - update counter
	    end,
	    case length(Rest) of % Now check rest of word to be counted...
		0 -> % 0 means finish - no more letters
		    Res2 = ets:lookup(Letter,totalCount), % But update a totalCount -er beforehand
		    case length(Res2) of
			0 ->
			    ets:insert(Letter,{totalCount,1});
			_ ->
			    {_,C2}=hd(Res2),
			    ets:insert(Letter,{totalCount,C2+1})
		    end;
		_ -> countCombos(Letter,[H2|Rest]) % Otherwise, keep going
	    end
    end.

%% Mapper method entry point
mapper(Letter,Parent) ->
    ets:new(list_to_atom([Letter]), [set, named_table]), % Create a new ETS
    {ok,Data}=file:read_file("only"++[Letter]++".txt"),
    SplitBinary = binary:split(Data,[<<"\n">>], [global]), % Read data from a file
    ListOfWords = lists:map(fun(X)-> binary_to_list(X) end, SplitBinary), % Turn it into a list
    mapper(Letter,Parent,tl(lists:reverse(ListOfWords))).

%% Mapper method with no more words. Return interim results
mapper(Letter,Parent,[]) ->
    ToSendBack = ets:tab2list(list_to_atom([Letter])), % Create a nice list
    Parent ! {intermediate, ToSendBack}; % No more data to read, send it back to the parent to be reduced.

%% Mapper method with words still to work on
mapper(Letter,Parent,[H|T]) ->
    % Map BIF not used because of desire to check words beforehand :)
    if 
	length(H)>1 -> % Good word
	    countCombos(list_to_atom([Letter]),H), % Count the combos, put in the ETS table
	    mapper(Letter,Parent,T); % Continue mapping on rest of list
	true -> % Bad word
	    mapper(Letter,Parent,T) % Continue mapping on rest of list
    end.