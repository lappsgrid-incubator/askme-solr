package org.lappsgrid.askme.solr

import edu.stanford.nlp.ling.CoreLabel
import edu.stanford.nlp.pipeline.CoreDocument
import edu.stanford.nlp.pipeline.CoreSentence
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.util.Pair
import org.lappsgrid.askme.core.model.Section
import org.lappsgrid.askme.core.model.Sentence
import org.lappsgrid.askme.core.model.Token
import org.lappsgrid.serialization.lif.Annotation

/**
 *
 */
class Stanford {

    StanfordCoreNLP pipeline;

    Stanford() {
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize,ssplit,pos,lemma");
        pipeline = new StanfordCoreNLP(props);
    }

    Section process(String text) {
        Section section = new Section()
        section.text = text

        CoreDocument document = new CoreDocument(text);
        try {
            pipeline.annotate(document);
        }
        catch (Exception e) {
            return section
        }

        // Process the sentences.
//        logger.trace("processing sentences")
        int id = 0;
        for (CoreSentence s : document.sentences()) {
            Pair<Integer,Integer> offsets = s.charOffsets();
            Sentence sentence = new Sentence()
            section.sentences.add(sentence)
            sentence.start = offsets.first()
            sentence.end = offsets.second()
            sentence.text = s.text()
            for (CoreLabel t : s.tokens()) {
                Token token = new Token()
                token.start = t.beginPosition()
                token.end = t.endPosition()
                token.word = t.word().toLowerCase()
                token.lemma = t.lemma().toLowerCase()
                token.pos = t.tag()
                token.category = t.category()
                sentence.tokens.add(token)
                section.tokens.add(token)
            }
        }
        return section
    }


    protected void set(Annotation a, String key, String value) {
        if (value == null) {
            return;
        }
        a.addFeature(key, value);
    }

}
