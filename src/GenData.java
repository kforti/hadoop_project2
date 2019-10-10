import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class GenData {

    public static void main(String[] args) throws IOException {
        String points_output_path = args[0]; //"data/points.txt";
        int num_points = Integer.parseInt(args[1]);
        String rects_output_path = args[2]; //"data/rectangles.txt";
        int num_rects = Integer.parseInt(args[3]);
        String point;
        String rect;
        GenData gen_data = new GenData();

        // Generate Points

        if (num_points > 0) {
            File points_output_file = new File(points_output_path);
            BufferedWriter points_writer = new BufferedWriter(new FileWriter(points_output_file));
            for (int i=0; i < num_points; i++) {
                point = gen_data.genPoint();
                points_writer.append(point + "\n");
            }
            points_writer.close();
        }

        // Generate Rectangles
        if (num_rects > 0) {
            File rects_output_file = new File(rects_output_path);
            BufferedWriter rects_writer = new BufferedWriter(new FileWriter(rects_output_file));
            for (int i=0; i < num_rects; i++) {
                rect = gen_data.genRectangle();
                rects_writer.append(rect + "\n");
            }
            rects_writer.close();
        }

    }

    public String genPoint(){
        String point;
        int x_cord;
        int y_cord;

        Random rand = new Random();
        x_cord = rand.nextInt(10001);
        y_cord = rand.nextInt(10001);
        point = String.format("%d,%d", x_cord, y_cord);
        return point;
    }

    public String genRectangle() {
        String rect;
        int x1_cord;
        int y1_cord;
        int height;
        int width;

        Random rand = new Random();
        x1_cord = rand.nextInt(10000 + 1);
        y1_cord = rand.nextInt(10000 + 1);

        width = rand.nextInt(100);//10000 + 1 - x1_cord);
        height = rand.nextInt(100);//10000 + 1 - y1_cord);
        rect = String.format("%d,%d,%d,%d", x1_cord, y1_cord, height, width);
        return rect;
    }

}
