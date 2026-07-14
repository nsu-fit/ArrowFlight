package net.surpin.data.arrowflight.server.services;

import org.junit.jupiter.api.Test;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.*;

class MetadataServiceTest {

    @Test
    void createLikePredicateNullMatchesAll() {
        Predicate<String> p = MetadataService.createLikePredicate(null);
        assertTrue(p.test("anything"));
        assertTrue(p.test(""));
    }

    @Test
    void createLikePredicatePercentMatchesAll() {
        Predicate<String> p = MetadataService.createLikePredicate("%");
        assertTrue(p.test("test_table"));
        assertTrue(p.test(""));
    }

    @Test
    void createLikePredicateExactMatch() {
        Predicate<String> p = MetadataService.createLikePredicate("my_table");
        assertTrue(p.test("my_table"));
        assertFalse(p.test("other_table"));
    }

    @Test
    void createLikePredicateWildcard() {
        Predicate<String> p = MetadataService.createLikePredicate("test_%");
        assertTrue(p.test("test_table"));
        assertTrue(p.test("test_other"));
        assertFalse(p.test("prod_table"));
    }

    @Test
    void createLikePredicateUnderscore() {
        Predicate<String> p = MetadataService.createLikePredicate("t_st");
        assertTrue(p.test("test"));
        assertTrue(p.test("tast"));
        assertFalse(p.test("toast"));
    }

    @Test
    void createLikePredicateLeadingWildcard() {
        Predicate<String> p = MetadataService.createLikePredicate("%_table");
        assertTrue(p.test("my_table"));
        assertTrue(p.test("a_table"));
        assertFalse(p.test("table"));
    }

    @Test
    void createLikePredicateRegexSafe() {
        // meta-characters in pattern are quoted, not interpreted as regex
        Predicate<String> p = MetadataService.createLikePredicate("t.bla");
        assertTrue(p.test("t.bla"));
        assertFalse(p.test("tXbla"));
    }
}
