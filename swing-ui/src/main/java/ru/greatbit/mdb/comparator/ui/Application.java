package ru.greatbit.mdb.comparator.ui;

import ru.greatbit.mdb.comparator.Error;

import javax.swing.*;
import javax.swing.filechooser.FileNameExtensionFilter;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import java.io.IOException;
import java.util.Date;
import java.util.List;

import static ru.greatbit.mdb.comparator.Comparer.compare;

public class Application extends JFrame {

    private Application() throws HeadlessException {
        createAndShowGUI();
    }

    private void createAndShowGUI() {
        JPanel panel = new JPanel();
        getContentPane().add(panel);

        panel.setLayout(null);

        JTextField file1 = new JTextField();
        file1.setBounds(20, 20, 190, 30);
        panel.add(file1);

        JTextField file2 = new JTextField();
        file2.setBounds(500, 20, 190, 30);
        panel.add(file2);

        JFileChooser fileSelector1 = new JFileChooser();
        fileSelector1.setFileFilter(
                new FileNameExtensionFilter("MS Access files", "mdb")
        );
        fileSelector1.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent event) {
                if (event.getActionCommand().equals(JFileChooser.APPROVE_SELECTION)){
                    file1.setText(fileSelector1.getSelectedFile().getAbsolutePath());
                }
            }
        });
        panel.add(fileSelector1);

        JFileChooser fileSelector2 = new JFileChooser();
        fileSelector2.setFileFilter(
                new FileNameExtensionFilter("MS Access files", "mdb")
        );
        fileSelector2.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent event) {
                if (event.getActionCommand().equals(JFileChooser.APPROVE_SELECTION)){
                    file2.setText(fileSelector2.getSelectedFile().getAbsolutePath());
                }
            }
        });
        panel.add(fileSelector2);

        JButton browseFile1 = new JButton("Browse");
        browseFile1.setBounds(20, 60, 100, 30);
        browseFile1.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent event) {
                fileSelector1.showOpenDialog(null);
            }
        });
        panel.add(browseFile1);

        JButton browseFile2 = new JButton("Browse");
        browseFile2.setBounds(500, 60, 100, 30);
        browseFile2.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent event) {
                fileSelector2.showOpenDialog(null);
            }
        });
        panel.add(browseFile2);

        JTextArea textArea = new JTextArea(10, 40);
        textArea.setBounds(20, 110, 700, 570);
        JScrollPane scroll = new JScrollPane(textArea);
        scroll.setBounds(20, 110, 700, 570);
        panel.add(scroll);

        JLabel toleranceLabel = new JLabel("Tolerance %");
        toleranceLabel.setBounds(250, 20, 100, 30);
        panel.add(toleranceLabel);

        JTextField tolerance = new JTextField();
        tolerance.setBounds(350, 20, 50, 30);
        tolerance.setText("0");
        panel.add(tolerance);

        JButton compareButton = new JButton("Compare");
        compareButton.setBounds(300, 60, 100, 30);
        compareButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent event) {
                try {
                    textArea.setText("Processing...");
                    executeComparison(
                            file1.getText(), file2.getText(),
                            Integer.parseInt(tolerance.getText()), textArea,
                            compareButton);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        panel.add(compareButton);

        setTitle("Compare MDB files");
        setSize(740, 700);
        setLocationRelativeTo(null);
        setDefaultCloseOperation(EXIT_ON_CLOSE);
    }

    private void executeComparison(String file1, String file2, int tolerance, JTextArea textArea, JButton compareButton) throws IOException {
        long now = new Date().getTime();
        compareButton.setEnabled(false);
        Runnable job = new Runnable() {
            @Override
            public void run(){
                List<Error> errors = null;
                try {
                    errors = compare(file1, file2, tolerance);
                } catch (Exception e) {
                    textArea.setText("Error occurred during execution: " + e);
                    compareButton.setEnabled(true);
                }
                long processedIn = (new Date().getTime() - now) / 1000;
                textArea.setText(getErrorText(errors, processedIn));
                compareButton.setEnabled(true);
            }
        };
        new Thread(job).start();
    }

    private String getErrorText(List<Error> errors, long processedIn) {
        StringBuffer sb = new StringBuffer();
        sb.append("Processed in ").append(processedIn).append(" seconds. \n");
        if (errors.size() == 0){
            sb.append("Files are identical");
            return sb.toString();
        } else {
            sb.append("Found ").append(errors.size()).append(" errors\n\n");
            errors.forEach(error -> {
                sb.append(error.getMessage()).append("\n\n");
            });

            return sb.toString();
        }
    }

    public static void main(String[] args) {
        new Thread(new Runnable() {
            @Override
            public void run() {
                Application application = new Application();
                application.setVisible(true);
            }
        }).start();
    }
}
