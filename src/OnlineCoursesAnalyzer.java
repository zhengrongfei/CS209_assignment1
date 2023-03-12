import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This is just a demo for you, please run it on JDK17.
 * This is just a demo, and you can extend and implement functions
 * based on this demo, or implement it in a different way.
 */
public class OnlineCoursesAnalyzer {

    List<Course> courses = new ArrayList<>();

    public OnlineCoursesAnalyzer(String datasetPath) {
        BufferedReader br = null;
        String line;
        try {
            br = new BufferedReader(new FileReader(datasetPath, StandardCharsets.UTF_8));
            br.readLine();
            while ((line = br.readLine()) != null) {
                String[] info = line.split(",(?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)", -1);
                Course course = new Course(info[0], info[1], new Date(info[2]), info[3], info[4], info[5],
                        Integer.parseInt(info[6]), Integer.parseInt(info[7]), Integer.parseInt(info[8]),
                        Integer.parseInt(info[9]), Integer.parseInt(info[10]), Double.parseDouble(info[11]),
                        Double.parseDouble(info[12]), Double.parseDouble(info[13]), Double.parseDouble(info[14]),
                        Double.parseDouble(info[15]), Double.parseDouble(info[16]), Double.parseDouble(info[17]),
                        Double.parseDouble(info[18]), Double.parseDouble(info[19]), Double.parseDouble(info[20]),
                        Double.parseDouble(info[21]), Double.parseDouble(info[22]));
                courses.add(course);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    //1
    public Map<String, Integer> getPtcpCountByInst() {
        Stream<Course> stream = courses.stream();
        Map<String, Integer> result = stream.sorted(Comparator.comparing(Course::getInstitution).reversed()).collect(Collectors.groupingBy(Course::getInstitution,Collectors.summingInt(Course::getParticipants)));
        return result;
    }

    //2

    public Map<String, Integer> getPtcpCountByInstAndSubject() {
        Stream<Course> stream = courses.stream();
        Map<String, Integer> list = stream.collect(Collectors.groupingBy(Course::getInsCourse,Collectors.summingInt(Course::getParticipants)));
        Map<String, Integer> sorted = new LinkedHashMap<>();
        list.entrySet().stream()
                .sorted(
                        Map.Entry.<String, Integer>comparingByValue()
                                .reversed()
                                .thenComparing(Map.Entry.comparingByKey()))
                .forEachOrdered(e -> sorted.put(e.getKey(), e.getValue()));
        return sorted;
    }

    //3
    public Map<String, List<List<String>>> getCourseListOfInstructor()
    {

        Map<String, List<List<String>>> result= new LinkedHashMap<>();
        for(Course c : courses) {
            boolean flag =c.instructors.contains(",");
            for (String ins : c.instructors.split(",")) {
                if(result.containsKey(ins.trim())){
                    List<List<String>> list=result.get(ins.trim());
                    if(flag){
                        list.get(1).add(c.title);
                    }
                    else{
                        list.get(0).add(c.title);
                    }
                }
                else{
                    List<List<String>> list=new ArrayList<>();
                    List<String> c1=new LinkedList<>();
                    List<String> c2= new LinkedList<>();
                    list.add(0,c1);
                    list.add(1,c2);
                    result.put(ins.trim(),list);
                    if(flag){
                        list.get(1).add(c.title);
                    }
                    else{
                        list.get(0).add(c.title);
                    }
                }
            }

        }
        Map<String, List<List<String>>> result0= new LinkedHashMap<>();
        for (Map.Entry<String, List<List<String>>> item : result.entrySet()){
            List<String> list0=item.getValue().get(0).stream().distinct().sorted().toList();
            List<String> list1=item.getValue().get(1).stream().distinct().sorted().toList();
            List<List<String>> list=new ArrayList<>();
            list.add(0,list0);
            list.add(1,list1);
            result0.put(item.getKey(),list);
        }
        return result0;
    }

    //4
    public List<String> getCourses(int topK, String by) {
        Stream<Course> stream = courses.stream();
        switch (by) {
            case "hours":
                return stream.sorted(Comparator.comparing(Course::getTotalHours).reversed()).filter(distinctByKey1(Course::getTitle)).limit(topK).map(Course::getTitle).toList();

            case "participants":
                return  stream.sorted(Comparator.comparing(Course::getParticipants).reversed()).filter(distinctByKey1(Course::getTitle)).limit(topK).distinct().map(Course::getTitle).toList();
            default:
                break;
        }

        return null;
    }
    static <T> Predicate<T> distinctByKey1(Function<? super T, ?> keyExtractor) {
        Map<Object, Boolean> seen = new ConcurrentHashMap<>();
        return t -> seen.putIfAbsent(keyExtractor.apply(t), Boolean.TRUE) == null;
    }




    //5
    public List<String> searchCourses(String courseSubject, double percentAudited, double totalCourseHours) {
        Stream<Course> stream = courses.stream();
        final String C=courseSubject.toLowerCase();
        List<String> list =stream.filter(e->e.percentAudited>=percentAudited && e.totalHours<=totalCourseHours && e.subject.toLowerCase().contains(C)).map(Course::getTitle).distinct().sorted().toList();
        return list;
    }

    //6
    public List<String> recommendCourses(int age, int gender, int isBachelorOrHigher) {
        Stream<Course> stream = courses.stream();
        Stream<Course> stream1 = courses.stream();
        Map<String,Course> course =new TreeMap();
        for(Course c : courses){
            if (course.containsKey(c.number)){
                if(c.launchDate.after(course.get(c.number).launchDate)){
                    course.put(c.number,c);
                }
            }
            else{
                course.put(c.number,c);
            }
        }
        Map<String,Double> map1=stream1.collect(Collectors.groupingBy(
                                Course::getNumber,
                                Collectors.averagingDouble(Course::getPercentDegree)));
        Stream<Course> stream2 = courses.stream();
        Map<String,Double> map2=stream2.collect(Collectors.groupingBy(
                Course::getNumber,
                Collectors.averagingDouble(Course::getPercentMale)));
        Stream<Course> stream3 = courses.stream();
        Map<String,Double> map3=stream3.collect(Collectors.groupingBy(
                Course::getNumber,
                Collectors.averagingDouble(Course::getMedianAge)));

        List<String> list =stream.sorted(
                (c1,c2)->{
                    double s1=(age-map3.get(c1.number))*(age-map3.get(c1.number))+(gender*100-map2.get(c1.number))*(gender*100-map2.get(c1.number))+(isBachelorOrHigher*100-map1.get(c1.number))*(isBachelorOrHigher*100-map1.get(c1.number));
                    double s2=(age-map3.get(c2.number))*(age-map3.get(c2.number))+(gender*100-map2.get(c2.number))*(gender*100-map2.get(c2.number))+(isBachelorOrHigher*100-map1.get(c2.number))*(isBachelorOrHigher*100-map1.get(c2.number));
                    if((s1- s2) < 0){
                        return -1;
                    }else if ((s1 - s2) > 0){
                        return 1;
                    }else {
                        return c1.getTitle().compareTo(c2.getTitle());
                    }
                }).map(e->course.get(e.number).title).distinct().limit(10).toList();
        return list;
    }


}

class Course {
    String institution;
    String number;
    Date launchDate;
    String title;
    String instructors;
    String subject;
    int year;
    int honorCode;
    int participants;
    int audited;
    int certified;
    double percentAudited;
    double percentCertified;
    double percentCertified50;
    double percentVideo;
    double percentForum;
    double gradeHigherZero;
    double totalHours;
    double medianHoursCertification;
    double medianAge;
    double percentMale;
    double percentFemale;
    double percentDegree;

    public Course(String institution, String number, Date launchDate,
                  String title, String instructors, String subject,
                  int year, int honorCode, int participants,
                  int audited, int certified, double percentAudited,
                  double percentCertified, double percentCertified50,
                  double percentVideo, double percentForum, double gradeHigherZero,
                  double totalHours, double medianHoursCertification,
                  double medianAge, double percentMale, double percentFemale,
                  double percentDegree) {
        this.institution = institution;
        this.number = number;
        this.launchDate = launchDate;
        if (title.startsWith("\"")) title = title.substring(1);
        if (title.endsWith("\"")) title = title.substring(0, title.length() - 1);
        this.title = title;
        if (instructors.startsWith("\"")) instructors = instructors.substring(1);
        if (instructors.endsWith("\"")) instructors = instructors.substring(0, instructors.length() - 1);
        this.instructors = instructors;
        if (subject.startsWith("\"")) subject = subject.substring(1);
        if (subject.endsWith("\"")) subject = subject.substring(0, subject.length() - 1);
        this.subject = subject;
        this.year = year;
        this.honorCode = honorCode;
        this.participants = participants;
        this.audited = audited;
        this.certified = certified;
        this.percentAudited = percentAudited;
        this.percentCertified = percentCertified;
        this.percentCertified50 = percentCertified50;
        this.percentVideo = percentVideo;
        this.percentForum = percentForum;
        this.gradeHigherZero = gradeHigherZero;
        this.totalHours = totalHours;
        this.medianHoursCertification = medianHoursCertification;
        this.medianAge = medianAge;
        this.percentMale = percentMale;
        this.percentFemale = percentFemale;
        this.percentDegree = percentDegree;
    }

    public String getInstitution() {
        return institution;
    }

    public int getParticipants() {
        return participants;
    }

    public String getSubject() {
        return subject;
    }



    public String getInsCourse(){
        return institution.concat("-").concat(subject);
    }

    public  double getTotalHours() {
        return totalHours;
    }

    public String getTitle() {
        return title;
    }


    public String getNumber() {
        return number;
    }

    public Date getLaunchDate() {
        return launchDate;
    }

    public double getMedianAge() {
        return medianAge;
    }

    public double getPercentMale(){
        return percentMale;
    }

    public double getPercentDegree() {
        return percentDegree;
    }
}
