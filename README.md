erlang_orswot
=============

Inspired by [Russel Brown's talk at EUC 2016](http://www.erlang-factory.com/euc2016/russell-brown), here is a simple proof-of-concept implementation in Erlang of an Observed Remove Set Without Tombstones (ORSWOT, a.k.a Optimized Conflict-free Replicated Set, a.k.a Optimized Add-Wins Set).

The specification of the set can be found in the paper [An optimized conflict-free replicated set](https://arxiv.org/abs/1210.3368), by Annette Bieniusa (INRIA Rocquencourt), Marek Zawirski (INRIA Rocquencourt, LIP6), Nuno Preguiça (CITI), Marc Shapiro (INRIA Rocquencourt, LIP6), Carlos Baquero (Universidade do Minho Departamento de Informática), Valter Balegas (CITI), Sérgio Duarte (CITI).

See figure 3 on page 7 of the paper for the formal specs, and also watch the recording of Russel Brown's talk for an excellent discussion of this concept in practice.


Build
-----
NOTE erlang_orswot uses [PropEr] (https://github.com/manopapad/proper), so please have its path in your ERL_LIBS environment variable

    $ rebar compile


Run PropEr tests
----------------
The behavior of the orswot makes it an excellent candidate for property-based testing.

To run all PropEr tests, just run:

    $ erlang_orswot:erlang_orswot_test().


This is just a wrapper for proper:module/1 and proper:module/2, so normal proper user_opts can be passed as well:

    $ erlang_orswot:erlang_orswot_test([{numtests, 50}]).
    