package part1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import part1.WordCountSHT.TweetCountSHT;

public class TokenizerMapperHT extends Mapper<Object, Text, Text, IntWritable>{

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(value.toString());
		while (itr.hasMoreTokens()) {
			word.set(itr.nextToken(" "));
			String tempstr = word.toString().toLowerCase();
			if(word.toString().contains("#")){
				context.getCounter(TweetCountSHT.TotalHashTags).increment(1L);
				if(tempstr.contains("#mh370")){ 
					word.set("#MH370");
					context.write(word, one);
				}
				if(tempstr.contains("#shameless")){
					word.set("#Shameless");
					context.write(word, one);
				}
				if(tempstr.contains("#mickeyrooney")){ 
					word.set("#MickeyRooney");
					context.write(word, one);
				}
				if(tempstr.contains("#easports")){
					word.set("#EASports");
					context.write(word, one);
				}
				if(tempstr.contains("#thankyoutaker")){ 
					word.set("#ThankYouTaker");
					context.write(word, one);
				}
				if(tempstr.contains("#fdny")){
					word.set("#FDNY");
					context.write(word, one);
				}
				if(tempstr.contains("#nypd")){ 
					word.set("#NYPD");
					context.write(word, one);
				}
				if(tempstr.contains("battlestargalactica")){
					word.set("#BattlestarGalactica");
					context.write(word, one);
				}
				if(tempstr.contains("#battlestargalactica")){
					word.set("#BattlestarGalactica");
					context.write(word, one);
				}
				//"#ken","#RuinARomCom","#HeartBleed"
				if(tempstr.contains("#ken")){ 
					word.set("#Ken");
					context.write(word, one);
				}
				if(tempstr.contains("ruinaromcom")){
					word.set("#RuinARomCom");
					context.write(word, one);
				}
				if(tempstr.contains("heartbleed")){
					word.set("#HeartBleed");
					context.write(word, one);
				}
				//
				if(tempstr.contains("#mtvmovieawards")){ 
					word.set("#MTVMovieAwards");
					context.write(word, one);
				}
				if(tempstr.contains("#snowinapril")){
					word.set("#SnowInApril");
					context.write(word, one);
				}
				if(tempstr.contains("#bloodmoon")){ 
					word.set("#BloodMoon");
					context.write(word, one);
				}
				if(tempstr.contains("#purplewedding")){
					word.set("#PurpleWedding");
					context.write(word, one);
				}
				if(tempstr.contains("#starwars")){ 
					word.set("#StarWars");
					context.write(word, one);
				}
				if(tempstr.contains("#gameofthrones")){
					word.set("#GameOfThrones");
					context.write(word, one);
				}
				if(tempstr.contains("#wwe")){ 
					word.set("#WWE");
					context.write(word, one);
				}
				if(tempstr.contains("#tetris")){
					word.set("#Tetris");
					context.write(word, one);
				}
				if(tempstr.contains("#bostonstrong")){ 
					word.set("#BostonStrong");
					context.write(word, one);
				}
				if(tempstr.contains("#mh192")){
					word.set("#MH192");
					context.write(word, one);
				}
				if(tempstr.contains("#goodfriday")){ 
					word.set("#GoodFriday");
					context.write(word, one);
				}
				if(tempstr.contains("#nbafinalspick")){
					word.set("#NBAFinalsPick");
					context.write(word, one);
				}
				if(tempstr.contains("#prayforsouthkorea")){ 
					word.set("#PrayForSouthKorea");
					context.write(word, one);
				}
				if(tempstr.contains("#worldstoughestjob")){
					word.set("#WorldsToughestJob");
					context.write(word, one);
				}
				if(tempstr.contains("#acdc")){ 
					word.set("#ACDC");
					context.write(word, one);
				}
				if(tempstr.contains("#mileycyrus")){
					word.set("#MileyCyrus");
					context.write(word, one);
				}
			}
		}
	}
}